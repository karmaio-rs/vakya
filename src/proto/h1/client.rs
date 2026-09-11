use super::{
    BodyMode, Expectation,
    bridge::{ReceiveEnd, SendBodyError, incoming, receive_body, send_body},
    decode::BodyDecoder,
    encode::{BodyEncoder, BodyMetadata, prepare_request_head},
    exchange::{Direction, Exchange, HeadAction, Outcome},
    head::{HeadParser, ParseOutcome, ResponseRole},
};
use crate::{
    Body, Error, ErrorKind, Response,
    body::pipe::Producer,
    client::{
        ResponseEvent,
        conn::http1::Builder,
        dispatch::{Job, Receiver},
        response::Control,
    },
    connection::ConnectionOutcome,
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
    mut requests: Receiver<B>,
    mut strategy: T,
    config: Builder,
) -> Result<ConnectionOutcome<I::ReadHalf, I::WriteHalf>, Error>
where
    I: IntoOwnedSplit,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<I::ReadHalf>,
{
    let (mut reader, mut writer) = io.into_split();
    let mut buffer = RecvBuffer::new(config.preferred_read, config.max_retained)?;
    let mut budget = WorkBudget::new();
    let mut parser = HeadParser::<ResponseRole>::response(config.protocol.head);
    while let Some(job) = requests.next().await {
        budget.step().await;
        let (next, persistence) = exchange(
            &mut reader,
            &mut writer,
            &mut strategy,
            &mut parser,
            buffer,
            job,
            &config,
        )
        .await?;
        buffer = next;

        match persistence {
            Outcome::Handoff(kind) => {
                drop(requests);
                return Ok(ConnectionOutcome::Upgraded(Upgraded::new(
                    reader,
                    writer,
                    buffer.take_all(),
                    kind,
                )));
            }
            Outcome::Close => break,
            Outcome::Reusable => {}
            Outcome::Active => return Err(Error::new(ErrorKind::Internal, "unsettled client exchange")),
        }

        requests.release();
    }
    // Close admission before awaiting transport shutdown so reservations cannot
    // wait for an exchange that will never run.
    drop(requests);
    writer.shutdown().await?;

    Ok(ConnectionOutcome::Closed)
}

async fn exchange<R, W, B, T>(
    reader: &mut R,
    writer: &mut W,
    strategy: &mut T,
    parser: &mut HeadParser<ResponseRole>,
    buffer: RecvBuffer,
    job: Job<B>,
    config: &Builder,
) -> Result<(RecvBuffer, Outcome), Error>
where
    W: AsyncWrite,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<R>,
{
    let Job {
        request,
        response,
        control,
    } = job;

    let mut response = Some(response);

    let result = exchange_inner(
        reader,
        writer,
        strategy,
        parser,
        buffer,
        request,
        &mut response,
        &control,
        config,
    )
    .await;

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
    config: &Builder,
) -> Result<(RecvBuffer, Outcome), Error>
where
    W: AsyncWrite,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<R>,
{
    let (parts, mut body) = request.into_parts();

    let metadata = BodyMetadata {
        size: body.size_hint(),
        trailers: body.trailer_hint(),
    };

    let (head, request) = prepare_request_head(
        parts.method,
        parts.uri,
        parts.version,
        parts.headers,
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
                karmaio::time::timeout(timeout, control.wait_continue()),
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
    config: &Builder,
) -> Result<(ReceiveEnd, RecvBuffer, Exchange), Error> {
    let head = loop {
        match parser.parse(buffer.bytes())? {
            ParseOutcome::Complete { head, consumed } => {
                buffer.consume(consumed)?;

                let (head, action) = state.receive_head(head)?;

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
                        *message.status_mut() = head.head.status;
                        *message.version_mut() = head.head.version;
                        *message.headers_mut() = head.head.headers;

                        offer_event(response, ResponseEvent::Informational(message), control).await?;

                        continue;
                    }
                    HeadAction::Upgrade(_) => {
                        control.allow_continue();
                        break head;
                    }
                }
            }
            ParseOutcome::NeedMore => {
                let (result, returned) = strategy.read(reader, buffer, Some(control.exchange.token())).await;
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

    let mut message = Response::new(incoming);
    *message.status_mut() = head.head.status;
    *message.version_mut() = head.head.version;
    *message.headers_mut() = head.head.headers;

    offer_event(response, ResponseEvent::Final(message), control).await?;
    response.take().expect("final response offered once").close();

    let (end, buffer) = match producer {
        Some(producer) => {
            receive_body(
                reader,
                strategy,
                buffer,
                BodyDecoder::with_limits(head.body, config.protocol.decode),
                producer,
                Some(control.exchange.token()),
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
