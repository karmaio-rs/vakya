use super::{
    BodyMode, Expectation,
    activity::{ReceiveResources, TransmitResources},
    body_io::{ReceiveEnd, incoming, map_send_error, receive_body_configured, send_body},
    decode::BodyDecoder,
    encode::{BodyEncoder, BodyMetadata, EncodeConfig, MessagePlan, prepare_request_head},
    exchange::{ActivityEnd, ExchangePolicy, HeadAction, Outcome},
    head::{HeadParser, ResponseRole, ValidatedRequestHead},
    header_case::HeaderCaseMap,
};
use crate::{
    Response,
    body::Body,
    body::pipe::Producer,
    client::{
        CloseReason, ExchangeOutcome,
        admission::{Job, Receiver},
        completion::CompletionGuard,
        conn::http1::config::Config as ClientConfig,
        response::{Control, ResponseEvent},
    },
    connection::{ConnectionOutcome, ConnectionParts},
    error::{Error, ErrorDetail, ErrorKind, TimeoutPhase},
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

    let mut terminal = None;
    let result = connection
        .instrument(connection.drive(
            &shutdown,
            pin!(run_inner(
                io,
                requests,
                strategy,
                config,
                &connection,
                &shutdown,
                &mut terminal
            )),
        ))
        .await;
    // The outer shutdown controller may replace a cancellation with its deadline
    // outcome. Publish only after that final originating-error composition.
    if let Some((completion, outcome)) = terminal {
        match result {
            Ok(result) => {
                completion.finish(Ok(outcome));
                Ok(result)
            }
            Err(error) => {
                let (driver, receipt) = error.split();
                completion.finish(Err(receipt));
                Err(driver)
            }
        }
    } else {
        result
    }
}

#[allow(clippy::too_many_arguments)] // Terminal receipt follows outer shutdown composition.
async fn run_inner<I, B, T>(
    io: I,
    mut requests: Receiver<B>,
    mut strategy: T,
    config: ClientConfig,
    connection: &crate::connection::ConnectionControl,
    shutdown: &karmaio::runtime::CancellationSource,
    terminal: &mut Option<(CompletionGuard, ExchangeOutcome)>,
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
        timeout: config.settings.write_progress_timeout,
    };
    let mut buffer = RecvBuffer::new(config.settings.preferred_read, config.settings.max_retained)?;
    let mut budget = WorkBudget::new();
    let mut parser = HeadParser::<ResponseRole>::response(config.settings.protocol.head)
        .with_preserved_header_case(config.settings.protocol.preserve_header_case);

    let mut can_recover = true;
    let mut exchanges = crate::trace::Exchanges::default();
    while let Some(job) = requests.next().await {
        budget.step().await;
        let Job {
            request,
            response,
            control,
            upgrade,
            completion,
        } = job;
        *terminal = Some((completion, ExchangeOutcome::Closed(CloseReason::MessagePolicy)));
        let span = exchanges.next();
        let settled = span
            .instrument(exchange(
                ReceiveResources {
                    reader: &mut reader,
                    strategy: &mut strategy,
                    parser: &mut parser,
                    buffer,
                },
                TransmitResources {
                    writer: &mut writer,
                    encoding: config.settings.protocol.encode,
                },
                request,
                response,
                control,
                &config,
                shutdown,
            ))
            .await?;
        span.outcome(match settled.outcome {
            Outcome::Handoff(_) => "handoff",
            Outcome::Close => "close",
            Outcome::Reusable => "reusable",
        });
        buffer = settled.buffer;

        match settled.outcome {
            Outcome::Handoff(kind) => {
                drop(requests);
                let upgraded = Upgraded::new(reader, writer.inner, buffer.take_all(), kind);
                let outcome = match upgrade.fulfill(upgraded) {
                    Ok(kind) => ConnectionOutcome::UpgradeClaimed(kind),
                    Err(upgraded) => ConnectionOutcome::Upgraded(upgraded),
                };
                terminal.as_mut().expect("active receipt").1 = ExchangeOutcome::Upgraded(kind);
                return Ok(outcome);
            }
            Outcome::Close => {
                can_recover = !matches!(
                    settled.close_reason,
                    CloseReason::BodyAbandonment | CloseReason::UploadAbort
                );
                terminal.as_mut().expect("active receipt").1 = ExchangeOutcome::Closed(settled.close_reason);
                break;
            }
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
        }

        if !requests.release() {
            terminal.as_mut().expect("active receipt").1 = ExchangeOutcome::Closed(if connection.stopping() {
                CloseReason::Shutdown
            } else {
                CloseReason::ApplicationClosure
            });
            break;
        }
        terminal
            .take()
            .expect("active receipt")
            .0
            .finish(Ok(ExchangeOutcome::Reusable));
    }
    // Close admission before awaiting transport shutdown so reservations cannot
    // wait for an exchange that will never run.
    drop(requests);
    if connection.recovering() && can_recover {
        return Ok(ConnectionOutcome::Recovered(ConnectionParts {
            reader,
            writer: writer.inner,
            read_ahead: buffer.take_all(),
        }));
    }
    writer.shutdown().with_cancellation(shutdown.token()).await?;

    Ok(ConnectionOutcome::Closed)
}

struct SettledExchange {
    buffer: RecvBuffer,
    outcome: Outcome,
    close_reason: CloseReason,
}

/// Receive returns policy with its recovered input only after body I/O settles.
struct ReceivedResponse {
    end: ReceiveEnd,
    buffer: RecvBuffer,
    policy: ExchangePolicy,
}

/// Prepared before either direction is polled. Preparation errors are local.
struct RequestUpload<B> {
    plan: MessagePlan<ValidatedRequestHead>,
    body: B,
    encoder: BodyEncoder,
    // Preserve application extensions until the complete exchange scope exits.
    extensions: http::Extensions,
}

impl<B: Body> RequestUpload<B> {
    fn prepare(
        request: http::Request<B>,
        encoding: EncodeConfig,
        max_informational: usize,
    ) -> Result<(Self, ExchangePolicy), Error> {
        let (parts, body) = request.into_parts();
        let metadata = BodyMetadata {
            size: body.size_hint(),
            trailers: body.trailer_hint(),
        };
        let plan = prepare_request_head(
            parts.method,
            parts.uri,
            parts.version,
            parts.headers,
            parts.extensions.get::<HeaderCaseMap>(),
            metadata,
            encoding,
        )?;
        let policy = ExchangePolicy::new(&plan.head, max_informational);
        let encoder = BodyEncoder::new(plan.head.body, metadata, encoding)?;
        Ok((
            Self {
                plan,
                body,
                encoder,
                extensions: parts.extensions,
            },
            policy,
        ))
    }
}

/// Borrow the producer from the exchange scope so it outlives both activities.
async fn send_request<W: AsyncWrite, B: Body>(
    writer: &mut W,
    bytes: bytes::Bytes,
    head: &ValidatedRequestHead,
    body: &mut B,
    encoder: &mut BodyEncoder,
    control: &Control,
    continue_wait: Option<std::time::Duration>,
) -> Result<(), Error>
where
    B::Error: std::error::Error + 'static,
{
    let mode = head.body;
    let (result, _) = write_all(writer, bytes, Some(control.upload.token()))
        .await
        .into_parts();
    result?;
    if head.expectation == Expectation::Continue
        && !matches!(mode, BodyMode::None | BodyMode::Fixed(0))
        && let Some(timeout) = continue_wait
    {
        writer.flush().with_cancellation(control.upload.token()).await?;

        // Receive releases permission before offering a 100 or final event;
        // an application pause cannot hold the upload's permission.
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

    if mode != BodyMode::None {
        send_body(writer, body, encoder, Some(control.upload.token()))
            .await
            .map_err(|error| map_send_error(error, "request body failed", "request production canceled"))?;
    }
    writer.flush().with_cancellation(control.upload.token()).await?;
    control.upload_complete.set(true);
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Disjoint exchange resources and connection cancellation.
async fn exchange<R, W, B, T>(
    receive_resources: ReceiveResources<'_, R, T, &mut HeadParser<ResponseRole>>,
    transmit: TransmitResources<'_, W>,
    request: crate::Request<B>,
    response: Producer<ResponseEvent, Error>,
    control: std::rc::Rc<Control>,
    config: &ClientConfig,
    shutdown: &karmaio::runtime::CancellationSource,
) -> Result<SettledExchange, Error>
where
    W: AsyncWrite,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<R>,
{
    let mut response = Some(response);

    let work = exchange_inner(receive_resources, transmit, request, &mut response, &control, config);
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
    receive_resources: ReceiveResources<'_, R, T, &mut HeadParser<ResponseRole>>,
    transmit: TransmitResources<'_, W>,
    request: http::Request<B>,
    response: &mut Option<Producer<ResponseEvent, Error>>,
    control: &Control,
    config: &ClientConfig,
) -> Result<SettledExchange, Error>
where
    W: AsyncWrite,
    B: Body,
    B::Error: std::error::Error + 'static,
    T: Receive<R>,
{
    let TransmitResources { writer, encoding } = transmit;
    let (upload, policy) = RequestUpload::prepare(request, encoding, config.settings.protocol.max_informational)?;
    // Keep request-owned resources in their original scope and drop order.
    let _extensions = upload.extensions;
    let mut body = upload.body;
    let plan = upload.plan;
    let mut encoder = upload.encoder;
    let send = send_request(
        writer,
        plan.bytes,
        &plan.head,
        &mut body,
        &mut encoder,
        control,
        config.continue_wait,
    )
    .with_cancellation(control.exchange.token());
    let receive = receive(receive_resources, policy, response, control, config);
    let mut send = pin!(send);
    let mut receive = pin!(receive);

    // The first terminal direction may request cancellation of its peer. Retain
    // and await that peer here before interpreting either result or reusing I/O.
    let (sent, received) = match race(receive.as_mut(), send.as_mut()).await {
        Race::First(received) => {
            if received.is_err()
                || received
                    .as_ref()
                    .is_ok_and(|received| matches!(received.end, ReceiveEnd::Abandoned))
            {
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

    finish_exchange(sent, received, explicit_abort(control))
}

/// Pure composition of already-settled results; cancellation happens above.
fn finish_exchange(
    sent: Result<(), Error>,
    received: Result<ReceivedResponse, Error>,
    upload_aborted: bool,
) -> Result<SettledExchange, Error> {
    // Prefer the originating failure over the other half's cancellation.
    let ReceivedResponse { end, buffer, policy } = match received {
        Ok(result) => result,
        Err(error) => {
            return Err(match sent {
                Err(sent) if !sent.is_canceled() && error.is_canceled() => sent,
                _ => error,
            });
        }
    };

    let response_end = match end {
        ReceiveEnd::Failed(error) => return Err(error),
        ReceiveEnd::Abandoned => {
            if let Err(error) = sent
                && !error.is_canceled()
            {
                return Err(error);
            }
            return Ok(SettledExchange {
                buffer,
                outcome: policy.finish(ActivityEnd::Retired, ActivityEnd::Retired)?,
                close_reason: CloseReason::BodyAbandonment,
            });
        }
        ReceiveEnd::Complete => ActivityEnd::Complete,
    };

    let request_end = match sent {
        Err(error) if !error.is_canceled() || !upload_aborted => return Err(error),
        Err(_) => ActivityEnd::Retired,
        // An abort can race with successful completion. Admission must still
        // retire: the application requested abort while upload was unfinished.
        Ok(()) if upload_aborted => ActivityEnd::Retired,
        Ok(()) => ActivityEnd::Complete,
    };
    Ok(SettledExchange {
        buffer,
        outcome: policy.finish(request_end, response_end)?,
        close_reason: if upload_aborted {
            CloseReason::UploadAbort
        } else {
            CloseReason::MessagePolicy
        },
    })
}

fn explicit_abort(control: &Control) -> bool {
    control.aborted.get()
}

#[allow(clippy::too_many_arguments)] // Reuse the connection parser workspace across exchanges.
async fn receive<R, T: Receive<R>>(
    resources: ReceiveResources<'_, R, T, &mut HeadParser<ResponseRole>>,
    mut state: ExchangePolicy,
    response: &mut Option<Producer<ResponseEvent, Error>>,
    control: &Control,
    config: &ClientConfig,
) -> Result<ReceivedResponse, Error> {
    let ReceiveResources {
        reader,
        strategy,
        parser,
        mut buffer,
    } = resources;
    let deadline = crate::io::deadline::configured_after(config.settings.head_timeout)?;
    let head = loop {
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(Error::new(ErrorKind::Timeout, "HTTP head deadline exceeded")
                .with_detail(ErrorDetail::Timeout(TimeoutPhase::Head)));
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
                        if let Some(info) = &config.settings.tls_info {
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
                                    .with_detail(ErrorDetail::Timeout(TimeoutPhase::Head))
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
    let incoming = incoming.with_drain_config(config.settings.drain);

    let mut message = Response::new(incoming);
    #[cfg(feature = "tls")]
    if let Some(info) = &config.settings.tls_info {
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
                BodyDecoder::with_limits(head.body, config.settings.protocol.decode),
                producer,
                Some(control.exchange.token()),
                config.settings.body_progress_timeout,
            )
            .await
        }
        None => (ReceiveEnd::Complete, buffer),
    };

    match end {
        ReceiveEnd::Failed(error) => Err(error),
        end => Ok(ReceivedResponse {
            end,
            buffer,
            policy: state,
        }),
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
