use super::{
    BodyMode, Expectation, Persistence,
    activity::{ReceiveResources, TransmitResources},
    body_io::{ReceiveEnd, SendBodyError, incoming, receive_body_configured, send_body},
    decode::BodyDecoder,
    encode::{BodyEncoder, BodyMetadata, prepare_response_head},
    exchange::{ActivityEnd, ExchangePolicy, Outcome},
    head::{HeadError, HeadParser, RequestHead, RequestRole, ValidatedRequestHead},
    header_case::HeaderCaseMap,
};
use crate::{
    body::{Body, Incoming, SizeHint, TrailerHint, incoming::IncomingProducer},
    connection::{ConnectionOutcome, ConnectionParts},
    error::{Error, ErrorDetail, ErrorKind, LimitKind, TimeoutPhase},
    future::{Race, WorkBudget, cancellable_wait, race},
    io::{
        recv::{ReadStatus, RecvBuffer},
        send::write_all,
        transport::Receive,
    },
    server::{
        conn::http1::config::Config as ServerConfig,
        context::{self, Command, InformationalReceiver, RequestContext},
    },
    service::Service,
    upgrade::{PendingUpgrade, Upgraded},
};
use http::{
    HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Version,
    header::{CONNECTION, DATE},
};
use karmaio::{
    io::{AsyncWrite, IntoOwnedSplit},
    runtime::{CancellationSource, FutureExt},
};
use std::{
    cell::Cell,
    future::poll_fn,
    pin::pin,
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) async fn run<I, S, B, T>(
    io: I,
    service: S,
    strategy: T,
    config: ServerConfig,
    connection: crate::connection::ConnectionControl,
) -> Result<ConnectionOutcome<I::ReadHalf, I::WriteHalf>, Error>
where
    I: IntoOwnedSplit,
    T: Receive<I::ReadHalf>,
    S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
    S::Error: std::error::Error + 'static,
    B: Body,
    B::Error: std::error::Error + 'static,
{
    let shutdown = karmaio::runtime::CancellationSource::new();

    connection
        .instrument(connection.drive(
            &shutdown,
            pin!(run_inner(io, service, strategy, config, &connection, &shutdown)),
        ))
        .await
}

async fn run_inner<I, S, B, T>(
    io: I,
    service: S,
    mut strategy: T,
    config: ServerConfig,
    connection: &crate::connection::ConnectionControl,
    shutdown: &karmaio::runtime::CancellationSource,
) -> Result<ConnectionOutcome<I::ReadHalf, I::WriteHalf>, Error>
where
    I: IntoOwnedSplit,
    T: Receive<I::ReadHalf>,
    S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
    S::Error: std::error::Error + 'static,
    B: Body,
    B::Error: std::error::Error + 'static,
{
    let (mut reader, writer) = io.into_split();
    let mut writer = crate::io::deadline::Write {
        inner: writer,
        timeout: config.write_progress_timeout,
    };
    let mut buffer = RecvBuffer::new(config.preferred_read, config.max_retained)?;
    let mut parser = HeadParser::<RequestRole>::request(config.protocol.head)
        .with_preserved_header_case(config.protocol.preserve_header_case);
    let mut date = DateCache::default();
    let mut budget = WorkBudget::new();
    // Graceful idle-read cancellation must leave transport shutdown usable.
    let idle_read = CancellationSource::new();

    let mut can_recover = true;
    let mut exchanges = crate::trace::Exchanges::default();
    'connection: loop {
        budget.step().await;

        if connection.stopping() {
            break;
        }

        let deadline = crate::io::deadline::configured_after(config.head_timeout)?;

        let head = loop {
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(Error::new(ErrorKind::Timeout, "HTTP head deadline exceeded")
                    .with_detail(ErrorDetail::Timeout(TimeoutPhase::Head)));
            }

            match parser.head_len(buffer.bytes()) {
                Ok(Some(consumed)) => {
                    let input = buffer.take_shared_prefix(consumed)?;
                    match parser.parse_shared(input).and_then(RequestHead::validate_received) {
                        Ok(head) => break head,
                        Err(error) => {
                            return Err(reply_illegal_head(&mut writer, error, &config, &mut date, shutdown).await);
                        }
                    }
                }
                Err(error) => {
                    return Err(reply_illegal_head(&mut writer, error, &config, &mut date, shutdown).await);
                }
                Ok(None) => {
                    let mut read = pin!(
                        strategy
                            .read_until(&mut reader, buffer, Some(idle_read.token()), deadline)
                            .with_cancellation(shutdown.token())
                    );
                    let mut stopped = pin!(connection.stopped());

                    let (result, returned) = match race(stopped.as_mut(), read.as_mut()).await {
                        Race::First(()) => {
                            idle_read.cancel();
                            let (result, returned) = read.await;
                            buffer = returned;
                            if let Err(error) = result
                                && !error.is_canceled()
                            {
                                return Err(error);
                            }
                            break 'connection;
                        }
                        Race::Second(result) => result,
                    };

                    buffer = returned;

                    if result? == ReadStatus::Eof {
                        if !buffer.bytes().is_empty() {
                            return Err(Error::new(
                                ErrorKind::InvalidMessage,
                                "connection ended inside a request head",
                            ));
                        }
                        break 'connection;
                    }
                }
            }
        };
        let span = exchanges.next();
        let (result, returned) = span
            .instrument(exchange(
                ReceiveResources {
                    reader: &mut reader,
                    strategy: &mut strategy,
                    buffer,
                    parser: BodyDecoder::with_limits(head.body, config.protocol.decode),
                },
                TransmitResources {
                    writer: &mut writer,
                    encoding: config.protocol.encode,
                },
                head,
                &service,
                &config,
                &mut date,
                shutdown,
            ))
            .await;
        buffer = returned;
        if let Ok(result) = &result {
            span.outcome(match result.outcome {
                Outcome::Handoff(_) => "handoff",
                Outcome::Close => "close",
                Outcome::Reusable => "reusable",
            });
        } else if let Err(error) = &result {
            span.failure(error.kind());
        }
        let ExchangeResult {
            outcome,
            upgrade,
            recoverable,
        } = result?;
        match outcome {
            Outcome::Handoff(kind) => {
                let upgraded = Upgraded::new(reader, writer.inner, buffer.take_all(), kind);
                return Ok(match upgrade.fulfill(upgraded) {
                    Ok(kind) => ConnectionOutcome::UpgradeClaimed(kind),
                    Err(upgraded) => ConnectionOutcome::Upgraded(upgraded),
                });
            }
            Outcome::Close => {
                can_recover = recoverable;
                break;
            }
            Outcome::Reusable => {}
        }
    }
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

// The reader and writer are borrowed exclusively by their own retained futures.
// Neither a final head nor a completed response implies request-body completion.
struct ExchangeResult {
    outcome: Outcome,
    upgrade: PendingUpgrade,
    recoverable: bool,
}

#[allow(clippy::too_many_arguments)] // Explicit disjoint connection resources; no shared driver allocation.
async fn exchange<R, W, S, B, T>(
    resources: ReceiveResources<'_, R, T, BodyDecoder>,
    mut transmit: TransmitResources<'_, W>,
    head: ValidatedRequestHead,
    service: &S,
    config: &ServerConfig,
    date: &mut DateCache,
    source: &CancellationSource,
) -> (Result<ExchangeResult, Error>, RecvBuffer)
where
    W: AsyncWrite,
    T: Receive<R>,
    S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
    S::Error: std::error::Error + 'static,
    B: Body,
    B::Error: std::error::Error + 'static,
{
    let mut state = ExchangePolicy::new(&head, config.protocol.max_informational);
    let method = head.head.method.clone();
    let version = head.head.version;
    let expect_continue = head.expectation == Expectation::Continue && version == Version::HTTP_11;
    let (context, informationals, upgrade) = context::channel(head.persistence == Persistence::Close);
    let close = informationals.close_flag();
    let (producer, body) = incoming(head.body);
    let request = request_message(head.head, body, config);
    let receive = receive_request(resources, producer, expect_continue, &informationals, config, source);
    let mut receive = pin!(receive);
    let mut received = None;

    // Select a final response while receive, the service, and the sole
    // informational writer progress. A service failure still settles receive.
    let response = {
        let mut call = pin!(call_service(
            &mut transmit,
            service,
            request,
            context,
            &informationals,
            version,
            config,
            source
        ));
        match race(receive.as_mut(), call.as_mut()).await {
            Race::First((ReceiveEnd::Failed(error), buffer)) => {
                source.cancel();
                let _ = call.await;
                return (Err(error), buffer);
            }
            Race::First(result) => {
                received = Some(result);
                call.await
            }
            Race::Second(result) => result,
        }
    };
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            source.cancel();
            let (_, buffer) = match received {
                Some(result) => result,
                None => receive.await,
            };
            return (Err(error), buffer);
        }
    };
    // Observe body abandonment caused by the completed service before committing
    // headers. Poll once without awaiting demand or delaying an early response.
    if received.is_none() {
        received = match poll_fn(|cx| Poll::Ready(receive.as_mut().poll(cx))).await {
            Poll::Ready(result) => Some(result),
            Poll::Pending => None,
        };
    }
    if let Some((ReceiveEnd::Failed(error), buffer)) = received {
        return (Err(error), buffer);
    }
    if matches!(received, Some((ReceiveEnd::Abandoned, _))) {
        close.set(true);
    }

    // Continue request reception during final transmission. Every path awaits
    // both retained directions before composing policy or returning the buffer.
    let sent = {
        let send = write_response(
            &mut transmit,
            response,
            &method,
            version,
            close,
            date,
            config,
            source,
            &mut state,
        );
        let mut send = pin!(send);
        if received.is_some() {
            send.await
        } else {
            match race(receive.as_mut(), send.as_mut()).await {
                Race::First((ReceiveEnd::Failed(error), buffer)) => {
                    source.cancel();
                    let _ = send.await;
                    return (Err(error), buffer);
                }
                Race::First(result) => {
                    received = Some(result);
                    send.await
                }
                Race::Second(result) => result,
            }
        }
    };
    if sent.is_err() {
        source.cancel();
    }
    let (end, buffer) = match received {
        Some(result) => result,
        None => receive.await,
    };
    if let Err(error) = sent {
        return (Err(error), buffer);
    }
    (finish_exchange(end, state, close.get(), upgrade), buffer)
}

fn request_message(head: RequestHead, body: Incoming, config: &ServerConfig) -> Request<Incoming> {
    let mut request = Request::new(body.with_drain_config(config.drain));
    #[cfg(feature = "tls")]
    if let Some(info) = &config.tls_info {
        request.extensions_mut().insert(info.clone());
    }
    if let Some(header_case) = head.header_case {
        request.extensions_mut().insert(header_case);
    }
    *request.method_mut() = head.method;
    *request.uri_mut() = head.target;
    *request.version_mut() = head.version;
    *request.headers_mut() = head.headers;
    request
}

/// Owns only receive resources; Continue permission precedes transport reads.
async fn receive_request<R, T: Receive<R>>(
    resources: ReceiveResources<'_, R, T, BodyDecoder>,
    producer: Option<IncomingProducer>,
    expect_continue: bool,
    informationals: &InformationalReceiver,
    config: &ServerConfig,
    source: &CancellationSource,
) -> (ReceiveEnd, RecvBuffer) {
    let ReceiveResources {
        reader,
        strategy,
        buffer,
        parser,
    } = resources;
    let Some(mut producer) = producer else {
        return (ReceiveEnd::Complete, buffer);
    };
    if expect_continue {
        match cancellable_wait(producer.demand(), Some(source.token())).await {
            Some(Ok(())) => {}
            Some(Err(_)) => return (ReceiveEnd::Abandoned, buffer),
            None => {
                return (
                    ReceiveEnd::fail(producer, Error::new(ErrorKind::Canceled, "request receive canceled")),
                    buffer,
                );
            }
        }
        match cancellable_wait(informationals.continue_permission(), Some(source.token())).await {
            Some(Ok(())) => {}
            Some(Err(error)) => return (ReceiveEnd::fail(producer, error), buffer),
            None => {
                return (
                    ReceiveEnd::fail(producer, Error::new(ErrorKind::Canceled, "Continue demand canceled")),
                    buffer,
                );
            }
        }
    }
    receive_body_configured(
        reader,
        strategy,
        buffer,
        parser,
        producer,
        Some(source.token()),
        config.body_progress_timeout,
    )
    .await
}

/// Called only after final transmission and request reception have settled.
fn finish_exchange(
    end: ReceiveEnd,
    mut state: ExchangePolicy,
    close: bool,
    upgrade: PendingUpgrade,
) -> Result<ExchangeResult, Error> {
    let request_end = match end {
        ReceiveEnd::Failed(error) => return Err(error),
        ReceiveEnd::Abandoned => ActivityEnd::Retired,
        ReceiveEnd::Complete => ActivityEnd::Complete,
    };
    if close {
        state.close_after_exchange();
    }
    state
        .finish(request_end, ActivityEnd::Complete)
        .map(|outcome| ExchangeResult {
            outcome,
            upgrade,
            recoverable: request_end == ActivityEnd::Complete,
        })
}

#[allow(clippy::too_many_arguments)] // Service and the sole head writer progress in the same scope.
async fn call_service<W, S, B>(
    transmit: &mut TransmitResources<'_, W>,
    service: &S,
    request: Request<Incoming>,
    context: RequestContext,
    informationals: &InformationalReceiver,
    version: Version,
    config: &ServerConfig,
    source: &CancellationSource,
) -> Result<Response<B>, Error>
where
    W: AsyncWrite,
    S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
    S::Error: std::error::Error + 'static,
{
    let mut call = pin!(cancellable_wait(service.call((request, context)), Some(source.token())));
    let mut heads = pin!(write_informationals(transmit, informationals, version, config, source));
    match race(call.as_mut(), heads.as_mut()).await {
        Race::First(result) => {
            informationals.select_final();
            if !matches!(result, Some(Ok(_))) {
                source.cancel();
            }
            // A selected final response cannot drop an in-flight informational
            // write, even if its application waiter has already disappeared.
            let written = heads.await;
            let response = match result {
                Some(Ok(response)) => response,
                Some(Err(error)) => return Err(Error::with_source(ErrorKind::Service, "HTTP service failed", error)),
                None => return Err(Error::new(ErrorKind::Canceled, "HTTP service canceled")),
            };
            written?;
            Ok(response)
        }
        Race::Second(result) => {
            source.cancel();
            let _ = call.await;
            result?;
            Err(Error::new(ErrorKind::Canceled, "informational writer stopped"))
        }
    }
}

async fn write_informationals<W: AsyncWrite>(
    transmit: &mut TransmitResources<'_, W>,
    receiver: &InformationalReceiver,
    version: Version,
    config: &ServerConfig,
    source: &CancellationSource,
) -> Result<(), Error> {
    use karmaio::runtime::FutureExt;
    let encoding = transmit.encoding;
    let writer = &mut *transmit.writer;
    let mut count = 0;
    loop {
        let command = match cancellable_wait(receiver.next(), Some(source.token())).await {
            Some(Some(command)) => command,
            Some(None) => return Ok(()),
            None => return Err(Error::new(ErrorKind::Canceled, "informational writer canceled")),
        };
        let automatic = matches!(command, Command::Continue);
        let response = match command {
            Command::Continue => Response::builder()
                .status(100)
                .body(())
                .expect("valid automatic Continue"),
            Command::Application(response) => response,
        };
        let head = (|| {
            if version != Version::HTTP_11 {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "informational responses require HTTP/1.1",
                ));
            }
            if count == config.protocol.max_informational {
                return Err(Error::new(ErrorKind::Limit, "too many informational responses")
                    .with_detail(ErrorDetail::OutgoingLimit(LimitKind::InformationalResponses)));
            }
            let (parts, ()) = response.into_parts();
            let original_case = parts.extensions.get::<HeaderCaseMap>();
            let head = prepare_response_head(
                parts.status,
                parts.version,
                parts.headers,
                original_case,
                BodyMetadata {
                    size: SizeHint::with_exact(0),
                    trailers: TrailerHint::None,
                },
                &Method::GET,
                encoding,
            )?;
            Ok(head)
        })();
        let head = match head {
            Ok(head) => head,
            Err(error) if !automatic => {
                receiver.complete(Err(error));
                continue;
            }
            Err(error) => return Err(error),
        };
        let (result, _) = write_all(writer, head.bytes, Some(source.token())).await.into_parts();
        let result = match result {
            Ok(_) => writer
                .flush()
                .with_cancellation(source.token())
                .await
                .map_err(Error::from),
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            if !automatic {
                let (driver, observer) = error.split();
                receiver.complete(Err(observer));
                return Err(driver);
            }
            return Err(error);
        }
        count += 1;
        if automatic {
            receiver.continued();
        } else {
            receiver.complete(Ok(()));
        }
    }
}

#[allow(clippy::too_many_arguments)] // Scoped writer state, all borrowed rather than cloned or shared.
async fn write_response<W: AsyncWrite, B: Body>(
    transmit: &mut TransmitResources<'_, W>,
    response: Response<B>,
    method: &Method,
    request_version: Version,
    close: &Cell<bool>,
    date: &mut DateCache,
    config: &ServerConfig,
    source: &CancellationSource,
    state: &mut ExchangePolicy,
) -> Result<Persistence, Error>
where
    B::Error: std::error::Error + 'static,
{
    let encoding = transmit.encoding;
    let writer = &mut *transmit.writer;
    let (mut parts, mut body) = response.into_parts();
    if parts.status.is_informational() && parts.status != http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "informational responses and transport handoff require their dedicated APIs",
        ));
    }
    if close.get() {
        parts.headers.append(CONNECTION, HeaderValue::from_static("close"));
    }
    if config.auto_date && !parts.headers.contains_key(DATE) {
        parts.headers.insert(DATE, date.value());
    }
    let version = if request_version == Version::HTTP_10 {
        Version::HTTP_10
    } else {
        parts.version
    };
    let metadata = BodyMetadata {
        size: body.size_hint(),
        trailers: body.trailer_hint(),
    };
    let original_case = parts.extensions.get::<HeaderCaseMap>();
    let plan = prepare_response_head(
        parts.status,
        version,
        parts.headers,
        original_case,
        metadata,
        method,
        encoding,
    )?;
    let mode = plan.head.body;
    let persistence = plan.head.persistence;
    if plan.head.upgrade.is_some() && close.get() {
        return Err(Error::new(
            ErrorKind::Upgrade,
            "closing exchange cannot transfer transport ownership",
        ));
    }
    state.sent_response(&plan.head)?;
    crate::trace::response_head(plan.head.head.status.as_u16());
    drop(plan.head); // The wire bytes and exchange policy now own what is needed.
    let mut encoder = BodyEncoder::new(mode, metadata, encoding)?;
    let (result, _) = write_all(writer, plan.bytes, Some(source.token())).await.into_parts();
    result?;
    // HEAD and 304 metadata describes a representation, not a producer to poll.
    if mode != BodyMode::None {
        send_body(writer, &mut body, &mut encoder, Some(source.token()))
            .await
            .map_err(|error| match error {
                SendBodyError::Body(error) => Error::with_source(ErrorKind::Body, "response body failed", error),
                SendBodyError::Encode(error) => error.into(),
                SendBodyError::Io(error) => error.into(),
                SendBodyError::Canceled => Error::new(ErrorKind::Canceled, "response production canceled"),
            })?;
    }
    // Custom buffered transports may need an explicit flush before the exchange
    // is considered sent. Apply the scope before this operation is first polled.
    use karmaio::runtime::FutureExt;
    writer.flush().with_cancellation(source.token()).await?;
    Ok(persistence)
}

async fn reply_illegal_head<W: AsyncWrite>(
    writer: &mut W,
    error: HeadError,
    config: &ServerConfig,
    date: &mut DateCache,
    shutdown: &CancellationSource,
) -> Error {
    let status = match error {
        HeadError::Limit(_) => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
        _ => StatusCode::BAD_REQUEST,
    };
    let failure = Error::from(error);
    if !config.auto_error_response {
        return failure;
    }

    let mut headers = HeaderMap::new();
    headers.insert(CONNECTION, HeaderValue::from_static("close"));
    if config.auto_date {
        headers.insert(DATE, date.value());
    }
    let Ok(head) = prepare_response_head(
        status,
        Version::HTTP_11,
        headers,
        None,
        BodyMetadata {
            size: SizeHint::with_exact(0),
            trailers: TrailerHint::None,
        },
        &Method::GET,
        config.protocol.encode,
    ) else {
        return failure;
    };

    let _ = async {
        let (result, _) = write_all(writer, head.bytes, Some(shutdown.token())).await.into_parts();
        result?;
        writer.flush().with_cancellation(shutdown.token()).await?;
        writer.shutdown().with_cancellation(shutdown.token()).await?;
        Ok::<(), Error>(())
    }
    .await;
    failure
}

#[derive(Default)]
struct DateCache(Option<(u64, HeaderValue)>);

impl DateCache {
    fn value(&mut self) -> HeaderValue {
        let now = SystemTime::now();
        let second = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        if let Some((cached, value)) = &self.0
            && *cached == second
        {
            return value.clone();
        }
        let value = HeaderValue::from_str(&httpdate::fmt_http_date(now)).expect("HTTP date is a valid header value");
        self.0 = Some((second, value.clone()));
        value
    }
}
