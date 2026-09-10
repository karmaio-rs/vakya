use super::{
    BodyMode, Expectation, Persistence,
    bridge::{ReceiveEnd, SendBodyError, incoming, receive_body, send_body},
    decode::BodyDecoder,
    encode::{BodyEncoder, BodyMetadata, encode_response_head},
    exchange::{Direction, Exchange, Outcome},
    head::{HeadParser, ParseOutcome, RequestRole, ValidatedRequestHead},
};
use crate::{
    Body, Error, ErrorKind, Incoming, Service,
    future::{Race, WorkBudget, race},
    io::{
        recv::{ReadStatus, RecvBuffer},
        send::write_all,
        transport::Receive,
    },
    server::{RequestContext, conn::http1::Builder},
};
use http::{
    HeaderValue, Method, Request, Response, Version,
    header::{CONNECTION, DATE},
};
use karmaio::{
    io::{AsyncWrite, IntoOwnedSplit},
    runtime::CancellationSource,
};
use std::{
    cell::Cell,
    future::poll_fn,
    pin::pin,
    rc::Rc,
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) async fn run<I, S, B, T>(io: I, service: S, mut strategy: T, config: Builder) -> Result<(), Error>
where
    I: IntoOwnedSplit,
    T: Receive<I::ReadHalf>,
    S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
    S::Error: std::error::Error + 'static,
    B: Body,
    B::Error: std::error::Error + 'static,
{
    let (mut reader, mut writer) = io.into_split();
    let mut buffer = RecvBuffer::new(config.preferred_read, config.max_retained)?;
    let mut parser = HeadParser::<RequestRole>::request(config.protocol.head);
    let mut date = DateCache::default();
    let mut budget = WorkBudget::new();
    loop {
        budget.step().await;
        let head = loop {
            match parser.parse(buffer.bytes())? {
                ParseOutcome::Complete { head, consumed } => {
                    buffer.consume(consumed)?;
                    break head.validate()?;
                }
                ParseOutcome::NeedMore => {
                    let (result, returned) = strategy.read(&mut reader, buffer, None).await;
                    buffer = returned;
                    if result? == ReadStatus::Eof {
                        if !buffer.bytes().is_empty() {
                            return Err(Error::new(
                                ErrorKind::InvalidMessage,
                                "connection ended inside a request head",
                            ));
                        }
                        writer.shutdown().await?;
                        return Ok(());
                    }
                }
            }
        };
        // Continue demand and informational serialization land together. Reject
        // this unsupported policy now instead of hanging a waiting peer.
        if head.expectation == Expectation::Continue && !matches!(head.body, BodyMode::None | BodyMode::Fixed(0)) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "100 Continue handling is not available yet",
            ));
        }
        let (result, returned) = exchange(
            &mut reader,
            &mut writer,
            &mut strategy,
            buffer,
            head,
            &service,
            &config,
            &mut date,
        )
        .await;
        buffer = returned;
        if result? == Persistence::Close {
            writer.shutdown().await?;
            return Ok(());
        }
    }
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
    config: &Builder,
    date: &mut DateCache,
) -> (Result<Persistence, Error>, RecvBuffer)
where
    W: AsyncWrite,
    T: Receive<R>,
    S: Service<(Request<Incoming>, RequestContext), Response = Response<B>>,
    S::Error: std::error::Error + 'static,
    B: Body,
    B::Error: std::error::Error + 'static,
{
    let source = CancellationSource::new();
    let mut state = Exchange::new(&head, config.protocol.max_informational);
    let method = head.head.method.clone();
    let version = head.head.version;
    let close = Rc::new(Cell::new(head.persistence == Persistence::Close));
    let (producer, body) = incoming(head.body);
    let mut request = Request::new(body);
    *request.method_mut() = head.head.method;
    *request.uri_mut() = head.head.target;
    *request.version_mut() = version;
    *request.headers_mut() = head.head.headers;
    let receive = async {
        match producer {
            Some(producer) => {
                receive_body(
                    reader,
                    strategy,
                    buffer,
                    BodyDecoder::with_limits(head.body, config.protocol.decode),
                    producer,
                    Some(source.token()),
                )
                .await
            }
            None => (ReceiveEnd::Complete, buffer),
        }
    };
    let mut receive = pin!(receive);
    let mut received = None;
    let response = {
        let mut call = pin!(service.call((request, RequestContext::new(close.clone()))));
        match race(receive.as_mut(), call.as_mut()).await {
            Race::First((ReceiveEnd::Failed(error), buffer)) => return (Err(error), buffer),
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
            return (
                Err(Error::with_source(ErrorKind::Service, "HTTP service failed", error)),
                buffer,
            );
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
        let send = write_response(writer, response, &method, version, &close, date, config, &source);
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
    let persistence = match sent {
        Ok(persistence) => persistence,
        Err(error) => return (Err(error), buffer),
    };
    match end {
        ReceiveEnd::Failed(error) => return (Err(error), buffer),
        ReceiveEnd::Abandoned => return (Ok(Persistence::Close), buffer),
        ReceiveEnd::Complete => {}
    }
    let result = (|| {
        state.sent_final(persistence)?;
        state.settle(Direction::Request, true)?;
        state.settle(Direction::Response, true)?;
        if close.get() {
            state.close_after_exchange();
        }
        Ok(if state.outcome() == Outcome::Reusable {
            Persistence::Reusable
        } else {
            Persistence::Close
        })
    })();
    (result, buffer)
}

#[allow(clippy::too_many_arguments)] // Scoped writer state, all borrowed rather than cloned or shared.
async fn write_response<W: AsyncWrite, B: Body>(
    writer: &mut W,
    response: Response<B>,
    method: &Method,
    request_version: Version,
    close: &Cell<bool>,
    date: &mut DateCache,
    config: &Builder,
    source: &CancellationSource,
) -> Result<Persistence, Error>
where
    B::Error: std::error::Error + 'static,
{
    let (mut parts, mut body) = response.into_parts();
    if parts.status.is_informational() || (method == Method::CONNECT && parts.status.is_success()) {
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
    let head = encode_response_head(
        parts.status,
        version,
        parts.headers,
        metadata,
        method,
        config.protocol.encode,
    )?;
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
