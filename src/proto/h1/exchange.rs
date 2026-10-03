use super::{
    Persistence, UpgradeKind,
    head::{ResponseHead, ValidatedRequestHead, ValidatedResponseHead},
    upgrade_protocols_match,
};
use crate::error::{Error, ErrorDetail, ErrorKind, LimitKind};
use http::{HeaderValue, Method, header::UPGRADE};

/// Successful terminal results returned by already-settled directional activities.
/// Retirement includes abandonment and explicit upload abort, never pending I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivityEnd {
    Complete,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponsePhase {
    AwaitingHead,
    Final,
    Upgrade(UpgradeKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HeadAction {
    Informational,
    Final,
    Upgrade(UpgradeKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Outcome {
    Reusable,
    Close,
    Handoff(UpgradeKind),
}

/// Message policy for one exchange, independent of directional activity lifetime.
///
/// Heads select policy; they do not assert I/O completion. The driver retains and
/// awaits both activities, including producer terminal checks and canceled owned
/// operations, before consuming this policy in `finish`. Originating failures
/// remain driver errors. No transport, body allocation, or scheduling lives here.
#[derive(Debug)]
pub(super) struct ExchangePolicy {
    phase: ResponsePhase,
    persistence: Persistence,
    method: Method,
    upgrade: Option<UpgradeKind>,
    offered_protocols: Vec<HeaderValue>,
    informational: usize,
    max_informational: usize,
}

impl ExchangePolicy {
    pub(super) fn new(request: &ValidatedRequestHead, max_informational: usize) -> Self {
        Self {
            phase: ResponsePhase::AwaitingHead,
            persistence: request.persistence,
            method: request.head.method.clone(),
            upgrade: request.upgrade,
            offered_protocols: if request.upgrade == Some(UpgradeKind::Protocol) {
                request.head.headers.get_all(UPGRADE).iter().cloned().collect()
            } else {
                Vec::new()
            },
            informational: 0,
            max_informational,
        }
    }

    /// Validate a peer response in the context of its request. A final response
    /// does not settle or cancel the upload. Drivers own Expect permission.
    pub(super) fn receive_head(&mut self, head: ResponseHead) -> Result<(ValidatedResponseHead, HeadAction), Error> {
        if self.phase != ResponsePhase::AwaitingHead {
            return Err(Error::new(
                ErrorKind::InvalidMessage,
                "response head after final response",
            ));
        }
        let head = head.validate(&self.method)?;
        if let Some(upgrade) = head.upgrade {
            if self.upgrade != Some(upgrade)
                || (upgrade == UpgradeKind::Protocol
                    && !upgrade_protocols_match(&self.offered_protocols, &head.head.headers))
            {
                return Err(Error::new(
                    ErrorKind::Upgrade,
                    "response did not match the requested upgrade",
                ));
            }
            self.phase = ResponsePhase::Upgrade(upgrade);
            return Ok((head, HeadAction::Upgrade(upgrade)));
        }
        if head.head.status.is_informational() {
            if self.informational == self.max_informational {
                return Err(Error::new(ErrorKind::Limit, "too many informational responses")
                    .with_detail(ErrorDetail::IncomingLimit(LimitKind::InformationalResponses)));
            }
            self.informational += 1;
            return Ok((head, HeadAction::Informational));
        }
        self.phase = ResponsePhase::Final;
        if head.persistence == Persistence::Close {
            self.persistence = Persistence::Close;
        }
        Ok((head, HeadAction::Final))
    }

    /// Record a validated final head committed by the server writer. Upload
    /// completion remains independent and is supplied to `finish`.
    pub(super) fn sent_final(&mut self, persistence: Persistence) -> Result<(), Error> {
        if self.phase != ResponsePhase::AwaitingHead {
            return Err(Error::new(ErrorKind::Internal, "invalid final response transition"));
        }
        self.phase = ResponsePhase::Final;
        if persistence == Persistence::Close {
            self.persistence = Persistence::Close;
        }
        Ok(())
    }

    /// Validate the server's chosen transition before committing any bytes.
    pub(super) fn sent_response(&mut self, head: &ValidatedResponseHead) -> Result<(), Error> {
        if let Some(upgrade) = head.upgrade {
            if self.phase != ResponsePhase::AwaitingHead
                || self.upgrade != Some(upgrade)
                || (upgrade == UpgradeKind::Protocol
                    && !upgrade_protocols_match(&self.offered_protocols, &head.head.headers))
            {
                return Err(Error::new(
                    ErrorKind::Upgrade,
                    "response did not match the requested upgrade",
                ));
            }

            self.phase = ResponsePhase::Upgrade(upgrade);

            Ok(())
        } else {
            self.sent_final(head.persistence)
        }
    }

    /// Prevent reuse without pretending that outstanding work has completed.
    pub(super) fn close_after_exchange(&mut self) {
        self.persistence = Persistence::Close;
    }

    /// Compose terminal activity results exactly once. The driver must await
    /// both directions before calling, even when either was asked to stop.
    pub(super) fn finish(self, request: ActivityEnd, response: ActivityEnd) -> Result<Outcome, Error> {
        if request == ActivityEnd::Retired || response == ActivityEnd::Retired {
            return Ok(Outcome::Close);
        }
        match self.phase {
            ResponsePhase::AwaitingHead => {
                Err(Error::new(ErrorKind::Internal, "exchange completed without final head"))
            }
            ResponsePhase::Upgrade(kind) => Ok(Outcome::Handoff(kind)),
            ResponsePhase::Final => Ok(match self.persistence {
                Persistence::Reusable => Outcome::Reusable,
                Persistence::Close => Outcome::Close,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::h1::{
        config::Config,
        head::{HeadParser, RequestRole, ResponseRole},
    };
    use bytes::Bytes;

    fn exchange(wire: &[u8]) -> ExchangePolicy {
        let config = Config::default();
        let mut parser = HeadParser::<RequestRole>::request(config.head);
        let consumed = parser.head_len(wire).unwrap().expect("complete request fixture");
        let head = parser.parse_shared(Bytes::copy_from_slice(&wire[..consumed])).unwrap();
        ExchangePolicy::new(&head.validate().unwrap(), config.max_informational)
    }

    fn response(wire: &[u8]) -> ResponseHead {
        let mut parser = HeadParser::<ResponseRole>::response(Config::default().head);
        let consumed = parser.head_len(wire).unwrap().expect("complete response fixture");
        parser.parse_shared(Bytes::copy_from_slice(&wire[..consumed])).unwrap()
    }

    #[test]
    fn early_final_selects_policy_without_directional_progress() {
        for status in ["200 OK", "400 Bad Request", "500 Internal Server Error"] {
            let mut state = exchange(
                b"POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\nExpect: 100-continue\r\n\r\n",
            );
            let (_, action) = state
                .receive_head(response(
                    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                ))
                .unwrap();
            assert_eq!(action, HeadAction::Final);
            assert_eq!(
                state.finish(ActivityEnd::Complete, ActivityEnd::Complete).unwrap(),
                Outcome::Reusable
            );
        }
    }

    #[test]
    fn retired_activity_prevents_handoff_or_reuse() {
        for request in [ActivityEnd::Complete, ActivityEnd::Retired] {
            for response_end in [ActivityEnd::Complete, ActivityEnd::Retired] {
                let mut state = exchange(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n");
                state
                    .receive_head(response(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
                    .unwrap();
                let outcome = state.finish(request, response_end).unwrap();
                assert_eq!(
                    outcome,
                    if request == ActivityEnd::Complete && response_end == ActivityEnd::Complete {
                        Outcome::Reusable
                    } else {
                        Outcome::Close
                    }
                );
            }
        }
    }

    #[test]
    fn clean_completion_requires_a_final_head() {
        let state = exchange(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n");
        assert_eq!(
            state
                .finish(ActivityEnd::Complete, ActivityEnd::Complete)
                .unwrap_err()
                .kind(),
            ErrorKind::Internal
        );
    }

    #[test]
    fn informational_ordering_and_budget_are_enforced() {
        let mut state =
            exchange(b"POST / HTTP/1.1\r\nHost: example.test\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\n");
        let early = b"HTTP/1.1 103 Early Hints\r\n\r\n";
        assert_eq!(
            state.receive_head(response(early)).unwrap().1,
            HeadAction::Informational
        );
        state.receive_head(response(b"HTTP/1.1 100 Continue\r\n\r\n")).unwrap();
        for _ in 2..16 {
            state.receive_head(response(early)).unwrap();
        }
        assert_eq!(
            state.receive_head(response(early)).unwrap_err().kind(),
            ErrorKind::Limit
        );

        let mut state = exchange(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n");
        state
            .receive_head(response(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
            .unwrap();
        assert_eq!(
            state.receive_head(response(early)).unwrap_err().kind(),
            ErrorKind::InvalidMessage
        );
    }

    #[test]
    fn upgrades_require_matching_intent_and_settled_directions() {
        for (request, reply, kind) in [
            (
                b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n".as_slice(),
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n".as_slice(),
                UpgradeKind::Protocol,
            ),
            (
                b"CONNECT example.test:443 HTTP/1.1\r\nHost: example.test\r\n\r\n".as_slice(),
                b"HTTP/1.1 200 OK\r\n\r\n".as_slice(),
                UpgradeKind::Tunnel,
            ),
        ] {
            let mut state = exchange(request);
            assert_eq!(
                state.receive_head(response(reply)).unwrap().1,
                HeadAction::Upgrade(kind)
            );
            assert_eq!(
                state.finish(ActivityEnd::Complete, ActivityEnd::Complete).unwrap(),
                Outcome::Handoff(kind)
            );
            let mut state = exchange(request);
            state.receive_head(response(reply)).unwrap();
            assert_eq!(
                state.finish(ActivityEnd::Retired, ActivityEnd::Complete).unwrap(),
                Outcome::Close
            );
        }
        for request in [
            b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n".as_slice(),
            b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: upgrade\r\nUpgrade: h2c\r\n\r\n".as_slice(),
        ] {
            let mut state = exchange(request);
            assert_eq!(
                state
                    .receive_head(response(
                        b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n"
                    ))
                    .unwrap_err()
                    .kind(),
                ErrorKind::Upgrade
            );
        }
    }

    #[test]
    fn close_delimited_and_explicit_close_exchanges_never_reuse() {
        for reply in [
            b"HTTP/1.1 200 OK\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice(),
        ] {
            let mut state = exchange(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n");
            state.receive_head(response(reply)).unwrap();
            assert_eq!(
                state.finish(ActivityEnd::Complete, ActivityEnd::Complete).unwrap(),
                Outcome::Close
            );
        }
    }
}
