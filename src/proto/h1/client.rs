use super::{
    BodyMode, Expectation,
    bridge::{ReceiveEnd, SendBodyError, incoming, receive_body_configured, send_body},
    decode::BodyDecoder,
    encode::{BodyEncoder, BodyMetadata, prepare_request_head},
    exchange::{Direction, Exchange, HeadAction, Outcome},
    head::{HeadParser, ResponseRole},
    header_case::HeaderCaseMap,
};
use crate::{
    Response,
    body::Body,
    body::pipe::Producer,
    client::{
        admission::{Job, Receiver},
        conn::http1::config::Config as ClientConfig,
        response::{Control, ResponseEvent},
    },
    connection::ConnectionOutcome,
    error::{Error, ErrorKind},
    future::{Race, WorkBudget, cancellable_wait, race},
    io::{
        recv::{ReadStatus, RecvBuffer},
        send::write_all,
        transport::Receive,
    },
    upgrade::Upgraded,
};
use karmaio::{
    io::{AsyncWrite, IntoOwnedSplit},
    runtime::FutureExt,
};
use std::pin::pin;

pub(crate) async fn run<I, B, T>(
    io: I,
    requests: Receiver<B>,
    strategy: T,
    config: ClientConfig,
    connection: crate::connection::ConnectionControl,
) -> Result<ConnectionOutcome<I::ReadHalf, I::WriteHalf>, Error>
where
    I: IntoOwnedSplit,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<I::ReadHalf>,
{
    let shutdown = karmaio::runtime::CancellationSource::new();

    connection
        .instrument(connection.drive(
            &shutdown,
            pin!(run_inner(io, requests, strategy, config, &connection, &shutdown)),
        ))
        .await
}

async fn run_inner<I, B, T>(
    io: I,
    mut requests: Receiver<B>,
    mut strategy: T,
    config: ClientConfig,
    connection: &crate::connection::ConnectionControl,
    shutdown: &karmaio::runtime::CancellationSource,
) -> Result<ConnectionOutcome<I::ReadHalf, I::WriteHalf>, Error>
where
    I: IntoOwnedSplit,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<I::ReadHalf>,
{
    let (mut reader, writer) = io.into_split();
    let mut writer = crate::io::deadline::Write {
        inner: writer,
        timeout: config.write_progress_timeout,
    };
    let mut buffer = RecvBuffer::new(config.preferred_read, config.max_retained)?;
    let mut budget = WorkBudget::new();
    let mut parser = HeadParser::<ResponseRole>::response(config.protocol.head)
        .with_preserved_header_case(config.protocol.preserve_header_case);

    let mut exchanges = crate::trace::Exchanges::default();
    while let Some(job) = requests.next().await {
        budget.step().await;
        let Job {
            request,
            response,
            control,
            upgrade,
        } = job;
        let span = exchanges.next();
        let (next, persistence) = span
            .instrument(exchange(
                &mut reader,
                &mut writer,
                &mut strategy,
                &mut parser,
                buffer,
                request,
                response,
                control,
                &config,
                shutdown,
            ))
            .await?;
        span.outcome(match persistence {
            Outcome::Handoff(_) => "handoff",
            Outcome::Close => "close",
            Outcome::Reusable => "reusable",
            Outcome::Active => "active",
        });
        buffer = next;

        match persistence {
            Outcome::Handoff(kind) => {
                drop(requests);
                let upgraded = Upgraded::new(reader, writer.inner, buffer.take_all(), kind);
                return Ok(match upgrade.fulfill(upgraded) {
                    Ok(kind) => ConnectionOutcome::UpgradeClaimed(kind),
                    Err(upgraded) => ConnectionOutcome::Upgraded(upgraded),
                });
            }
            Outcome::Close => break,
            Outcome::Reusable => {
                // No other request has been sent: read-ahead cannot belong to
                // a later response. Only handoff may transfer this prefix.
                if !buffer.bytes().is_empty() {
                    return Err(Error::new(
                        ErrorKind::InvalidMessage,
                        "unsolicited data after completed HTTP response",
                    ));
                }
            }
            Outcome::Active => return Err(Error::new(ErrorKind::Internal, "unsettled client exchange")),
        }

        if connection.stopping() {
            break;
        }

        requests.release();
    }
    // Close admission before awaiting transport shutdown so reservations cannot
    // wait for an exchange that will never run.
    drop(requests);
    writer.shutdown().with_cancellation(shutdown.token()).await?;

    Ok(ConnectionOutcome::Closed)
}

#[allow(clippy::too_many_arguments)] // Disjoint exchange resources and connection cancellation.
async fn exchange<R, W, B, T>(
    reader: &mut R,
    writer: &mut W,
    strategy: &mut T,
    parser: &mut HeadParser<ResponseRole>,
    buffer: RecvBuffer,
    request: crate::Request<B>,
    response: Producer<ResponseEvent, Error>,
    control: std::rc::Rc<Control>,
    config: &ClientConfig,
    shutdown: &karmaio::runtime::CancellationSource,
) -> Result<(RecvBuffer, Outcome), Error>
where
    W: AsyncWrite,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<R>,
{
    let mut response = Some(response);

    let work = exchange_inner(
        reader,
        writer,
        strategy,
        parser,
        buffer,
        request,
        &mut response,
        &control,
        config,
    );
    let result = {
        let mut work = pin!(work);
        let mut canceled = pin!(shutdown.token().cancelled());

        match race(canceled.as_mut(), work.as_mut()).await {
            Race::First(()) => {
                control.cancel();
                work.await
            }
            Race::Second(result) => result,
        }
    };

    match result {
        Err(error) => {
            if let Some(response) = response {
                let (driver, observer) = error.split();
                response.fail(observer);
                Err(driver)
            } else {
                Err(error)
            }
        }
        Ok(result) => Ok(result),
    }
}

#[allow(clippy::too_many_arguments)] // Disjoint scoped halves and observation ownership.
async fn exchange_inner<R, W, B, T>(
    reader: &mut R,
    writer: &mut W,
    strategy: &mut T,
    parser: &mut HeadParser<ResponseRole>,
    buffer: RecvBuffer,
    request: http::Request<B>,
    response: &mut Option<Producer<ResponseEvent, Error>>,
    control: &Control,
    config: &ClientConfig,
) -> Result<(RecvBuffer, Outcome), Error>
where
    W: AsyncWrite,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<R>,
{
    let (parts, mut body) = request.into_parts();
    let original_case = parts.extensions.get::<HeaderCaseMap>();

    let metadata = BodyMetadata {
        size: body.size_hint(),
        trailers: body.trailer_hint(),
    };

    let (head, request) = prepare_request_head(
        parts.method,
        parts.uri,
        parts.version,
        parts.headers,
        original_case,
        metadata,
        config.protocol.encode,
    )?;

    let state = Exchange::new(&request, config.protocol.max_informational);
    let mut encoder = BodyEncoder::new(head.mode, metadata, config.protocol.encode)?;

    let send = async {
        let (result, _) = write_all(writer, head.bytes, Some(control.upload.token()))
            .await
            .into_parts();
        result?;
        if request.expectation == Expectation::Continue
            && !matches!(head.mode, BodyMode::None | BodyMode::Fixed(0))
            && let Some(timeout) = config.continue_wait
        {
            writer.flush().with_cancellation(control.upload.token()).await?;

            // The read half releases permission before offering an observed 100
            // or final event; an application pause cannot hold that permission.
            let waited = cancellable_wait(
                karmaio::time::timeout_at(
                    crate::io::deadline::configured_after(Some(timeout))?.expect("configured Continue duration"),
                    control.wait_continue(),
                ),
                Some(control.upload.token()),
            )
            .await;

            if waited.is_none() {
                return Err(Error::new(ErrorKind::Canceled, "Continue wait canceled"));
            }
        }

        if head.mode != BodyMode::None {
            send_body(writer, &mut body, &mut encoder, Some(control.upload.token()))
                .await
                .map_err(map_send_error)?;
        }

        writer.flush().with_cancellation(control.upload.token()).await?;
        control.upload_complete.set(true);

        Ok::<(), Error>(())
    }
    .with_cancellation(control.exchange.token());

    let receive = receive(reader, strategy, parser, buffer, state, response, control, config);
    let mut send = pin!(send);
    let mut receive = pin!(receive);

    let (sent, received) = match race(receive.as_mut(), send.as_mut()).await {
        Race::First(received) => {
            if received.is_err() || matches!(received, Ok((ReceiveEnd::Abandoned, _, _))) {
                control.cancel();
            }
            (send.await, received)
        }
        Race::Second(sent) => {
            // Explicit upload abort preserves response receiving. Every other
            // send failure cancels and settles the retained read before return.
            if sent
                .as_ref()
                .is_err_and(|error| !error.is_canceled() || !explicit_abort(control))
            {
                control.cancel();
            }
            (sent, receive.await)
        }
    };

    // Prefer the originating failure over the other half's cancellation.
    let (end, buffer, mut state) = match received {
        Ok(result) => result,
        Err(error) => {
            return Err(match sent {
                Err(sent) if !sent.is_canceled() && error.is_canceled() => sent,
                _ => error,
            });
        }
    };

    match end {
        ReceiveEnd::Failed(error) => return Err(error),
        ReceiveEnd::Abandoned => {
            if let Err(error) = sent
                && !error.is_canceled()
            {
                return Err(error);
            }
            return Ok((buffer, Outcome::Close));
        }
        ReceiveEnd::Complete => {}
    }

    if let Err(error) = sent {
        if !error.is_canceled() || !explicit_abort(control) {
            return Err(error);
        }
        state.settle(Direction::Request, false)?;
    } else {
        state.settle(Direction::Request, true)?;
    }

    state.settle(Direction::Response, true)?;

    // Cancellation can race with successful completion. The abort decision was
    // made while upload was unfinished, so even a successful final completion
    // must not reopen admission.
    if explicit_abort(control) {
        state.close_after_exchange();
    }

    Ok((
        buffer,
        if explicit_abort(control) {
            Outcome::Close
        } else {
            state.outcome()
        },
    ))
}

fn explicit_abort(control: &Control) -> bool {
    control.aborted.get()
}

#[allow(clippy::too_many_arguments)] // Reuse the connection parser workspace across exchanges.
async fn receive<R, T: Receive<R>>(
    reader: &mut R,
    strategy: &mut T,
    parser: &mut HeadParser<ResponseRole>,
    mut buffer: RecvBuffer,
    mut state: Exchange,
    response: &mut Option<Producer<ResponseEvent, Error>>,
    control: &Control,
    config: &ClientConfig,
) -> Result<(ReceiveEnd, RecvBuffer, Exchange), Error> {
    let deadline = crate::io::deadline::configured_after(config.head_timeout)?;
    let head = loop {
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(Error::new(ErrorKind::Timeout, "HTTP head deadline exceeded"));
        }

        match parser.head_len(buffer.bytes())? {
            Some(consumed) => {
                let input = buffer.take_shared_prefix(consumed)?;
                let (head, action) = state.receive_head(parser.parse_shared(input)?)?;

                crate::trace::response_head(head.head.status.as_u16());
                match action {
                    HeadAction::Final => {
                        control.allow_continue();
                        break head;
                    }
                    HeadAction::Informational => {
                        if head.head.status == 100 {
                            control.allow_continue();
                        }

                        let mut message = Response::new(());
                        #[cfg(feature = "tls")]
                        if let Some(info) = &config.tls_info {
                            message.extensions_mut().insert(info.clone());
                        }
                        if let Some(header_case) = head.head.header_case {
                            message.extensions_mut().insert(header_case);
                        }
                        *message.status_mut() = head.head.status;
                        *message.version_mut() = head.head.version;
                        *message.headers_mut() = head.head.headers;

                        let offer = offer_event(response, ResponseEvent::Informational(message), control);
                        match deadline {
                            Some(deadline) => karmaio::time::timeout_at(deadline, offer).await.map_err(|_| {
                                Error::new(ErrorKind::Timeout, "final response head deadline exceeded")
                            })??,
                            None => offer.await?,
                        }

                        continue;
                    }
                    HeadAction::Upgrade(_) => {
                        control.allow_continue();
                        break head;
                    }
                }
            }
            None => {
                let (result, returned) = strategy
                    .read_until(reader, buffer, Some(control.exchange.token()), deadline)
                    .await;
                buffer = returned;

                if result? == ReadStatus::Eof {
                    return Err(Error::new(
                        if buffer.bytes().is_empty() {
                            ErrorKind::Closed
                        } else {
                            ErrorKind::InvalidMessage
                        },
                        "connection ended before a final response head",
                    ));
                }
            }
        }
    };

    let (producer, incoming) = incoming(head.body);
    let incoming = incoming.with_drain_config(config.drain);

    let mut message = Response::new(incoming);
    #[cfg(feature = "tls")]
    if let Some(info) = &config.tls_info {
        message.extensions_mut().insert(info.clone());
    }
    if let Some(header_case) = head.head.header_case {
        message.extensions_mut().insert(header_case);
    }
    *message.status_mut() = head.head.status;
    *message.version_mut() = head.head.version;
    *message.headers_mut() = head.head.headers;

    offer_event(response, ResponseEvent::Final(message), control).await?;
    response.take().expect("final response offered once").close();

    let (end, buffer) = match producer {
        Some(producer) => {
            receive_body_configured(
                reader,
                strategy,
                buffer,
                BodyDecoder::with_limits(head.body, config.protocol.decode),
                producer,
                Some(control.exchange.token()),
                config.body_progress_timeout,
            )
            .await
        }
        None => (ReceiveEnd::Complete, buffer),
    };

    match end {
        ReceiveEnd::Failed(error) => Err(error),
        end => Ok((end, buffer, state)),
    }
}

async fn offer_event(
    response: &mut Option<Producer<ResponseEvent, Error>>,
    event: ResponseEvent,
    control: &Control,
) -> Result<(), Error> {
    let observed = cancellable_wait(
        response.as_mut().expect("response stream remains open").offer(event),
        Some(control.exchange.token()),
    )
    .await;

    if !matches!(observed, Some(Ok(()))) {
        return Err(Error::new(ErrorKind::Canceled, "response observation abandoned"));
    }

    Ok(())
}

fn map_send_error<E: std::error::Error + 'static>(error: SendBodyError<E>) -> Error {
    match error {
        SendBodyError::Body(error) => Error::with_source(ErrorKind::Body, "request body failed", error),
        SendBodyError::Encode(error) => error.into(),
        SendBodyError::Io(error) => error.into(),
        SendBodyError::Canceled => Error::new(ErrorKind::Canceled, "request production canceled"),
    }
}
