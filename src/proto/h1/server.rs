use super::{
    BodyMode, Expectation, Persistence,
    bridge::{ReceiveEnd, SendBodyError, incoming, receive_body_configured, send_body},
    decode::BodyDecoder,
    encode::{BodyEncoder, BodyMetadata, encode_response_head, prepare_response_head},
    exchange::{Direction, Exchange, Outcome},
    head::{HeadParser, ParseOutcome, RequestRole, ValidatedRequestHead},
};
use crate::{
    Body, Error, ErrorKind, Incoming, Service,
    connection::ConnectionOutcome,
    engine::{
        config::ServerConfig,
        server::{self as context, Command, InformationalReceiver, RequestContext},
    },
    future::{Race, WorkBudget, cancellable_wait, race},
    io::{
        recv::{ReadStatus, RecvBuffer},
        send::write_all,
        transport::Receive,
    },
    upgrade::Upgraded,
};
use http::{
    HeaderValue, Method, Request, Response, Version,
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
        .drive(
            &shutdown,
            pin!(run_inner(io, service, strategy, config, &connection, &shutdown)),
        )
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
    let mut parser = HeadParser::<RequestRole>::request(config.protocol.head);
    let mut date = DateCache::default();
    let mut budget = WorkBudget::new();
    // Graceful idle-read cancellation must leave transport shutdown usable.
    let idle_read = CancellationSource::new();

    'connection: loop {
        budget.step().await;

        if connection.stopping() {
            break;
        }

        let deadline = crate::io::deadline::configured_after(config.head_timeout)?;

        let head = loop {
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(Error::new(ErrorKind::Timeout, "HTTP head deadline exceeded"));
            }

            match parser.parse(buffer.bytes())? {
                ParseOutcome::Complete { head, consumed } => {
                    buffer.consume(consumed)?;
                    break head.validate()?;
                }
                ParseOutcome::NeedMore => {
                    let mut read = pin!(
                        strategy
                            .read_until(&mut reader, buffer, Some(idle_read.token()), deadline)
                            .with_cancellation(shutdown.token())
                    );
                    let mut stopped = pin!(connection.stopped());

                    let (result, returned) = match race(stopped.as_mut(), read.as_mut()).await {
                        Race::First(()) => {
                            idle_read.cancel();
                            let (result, _) = read.await;
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
        let (result, returned) = exchange(
            &mut reader,
            &mut writer,
            &mut strategy,
            buffer,
            head,
            &service,
            &config,
            &mut date,
            shutdown,
        )
        .await;
        buffer = returned;
        match result? {
            Outcome::Handoff(kind) => {
                return Ok(ConnectionOutcome::Upgraded(Upgraded::new(
                    reader,
                    writer.inner,
                    buffer.take_all(),
                    kind,
                )));
            }
            Outcome::Close => break,
            Outcome::Reusable => {}
            Outcome::Active => return Err(Error::new(ErrorKind::Internal, "unsettled server exchange")),
        }
    }
    writer.shutdown().with_cancellation(shutdown.token()).await?;
    Ok(ConnectionOutcome::Closed)
}

// The reader and writer are borrowed exclusively by their own retained futures.
// Neither a final head nor a completed response implies request-body completion.
#[allow(clippy::too_many_arguments)] // Explicit disjoint connection resources; no shared driver allocation.
async fn exchange<R, W, S, B, T>(
    reader: &mut R,
    writer: &mut W,
    strategy: &mut T,
    buffer: RecvBuffer,
    head: ValidatedRequestHead,
    service: &S,
    config: &ServerConfig,
    date: &mut DateCache,
    source: &CancellationSource,
) -> (Result<Outcome, Error>, RecvBuffer)
where
    W: AsyncWrite,
    T: Receive<R>,
    S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
    S::Error: std::error::Error + 'static,
    B: Body,
    B::Error: std::error::Error + 'static,
{
    let mut state = Exchange::new(&head, config.protocol.max_informational);
    let method = head.head.method.clone();
    let version = head.head.version;
    let (context, informationals) = context::channel(head.persistence == Persistence::Close);
    let close = informationals.close_flag();
    let (producer, body) = incoming(head.body);
    let mut request = Request::new(body);
    #[cfg(feature = "tls")]
    if let Some(info) = &config.tls_info {
        request.extensions_mut().insert(info.clone());
    }
    *request.method_mut() = head.head.method;
    *request.uri_mut() = head.head.target;
    *request.version_mut() = version;
    *request.headers_mut() = head.head.headers;
    let receive = async {
        match producer {
            Some(mut producer) => {
                if head.expectation == Expectation::Continue && version == Version::HTTP_11 {
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
                    BodyDecoder::with_limits(head.body, config.protocol.decode),
                    producer,
                    Some(source.token()),
                    config.body_progress_timeout,
                )
                .await
            }
            None => (ReceiveEnd::Complete, buffer),
        }
    };
    let mut receive = pin!(receive);
    let mut received = None;
    let response = {
        let mut call = pin!(call_service(
            writer,
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
    let sent = {
        let send = write_response(
            writer, response, &method, version, close, date, config, source, &mut state,
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
    match sent {
        Ok(_) => {}
        Err(error) => return (Err(error), buffer),
    };
    match end {
        ReceiveEnd::Failed(error) => return (Err(error), buffer),
        ReceiveEnd::Abandoned => return (Ok(Outcome::Close), buffer),
        ReceiveEnd::Complete => {}
    }
    let result = (|| {
        state.settle(Direction::Request, true)?;
        state.settle(Direction::Response, true)?;
        if close.get() {
            state.close_after_exchange();
        }
        Ok(state.outcome())
    })();
    (result, buffer)
}

#[allow(clippy::too_many_arguments)] // Service and the sole head writer progress in the same scope.
async fn call_service<W, S, B>(
    writer: &mut W,
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
    let mut heads = pin!(write_informationals(writer, informationals, version, config, source));
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
    writer: &mut W,
    receiver: &InformationalReceiver,
    version: Version,
    config: &ServerConfig,
    source: &CancellationSource,
) -> Result<(), Error> {
    use karmaio::runtime::FutureExt;
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
                return Err(Error::new(ErrorKind::Limit, "too many informational responses"));
            }
            let (parts, ()) = response.into_parts();
            let head = encode_response_head(
                parts.status,
                parts.version,
                parts.headers,
                BodyMetadata {
                    size: crate::SizeHint::with_exact(0),
                    trailers: crate::TrailerHint::None,
                },
                &Method::GET,
                config.protocol.encode,
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
    writer: &mut W,
    response: Response<B>,
    method: &Method,
    request_version: Version,
    close: &Cell<bool>,
    date: &mut DateCache,
    config: &ServerConfig,
    source: &CancellationSource,
    state: &mut Exchange,
) -> Result<Persistence, Error>
where
    B::Error: std::error::Error + 'static,
{
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
    let (head, validated) = prepare_response_head(
        parts.status,
        version,
        parts.headers,
        metadata,
        method,
        config.protocol.encode,
    )?;
    if head.upgrade.is_some() && close.get() {
        return Err(Error::new(
            ErrorKind::Upgrade,
            "closing exchange cannot transfer transport ownership",
        ));
    }
    state.sent_response(&validated)?;
    drop(validated); // The encoded bytes and exchange decisions now own what is needed.
    let mut encoder = BodyEncoder::new(head.mode, metadata, config.protocol.encode)?;
    let (result, _) = write_all(writer, head.bytes, Some(source.token())).await.into_parts();
    result?;
    // HEAD and 304 metadata describes a representation, not a producer to poll.
    if head.mode != BodyMode::None {
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
    Ok(head.persistence)
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
