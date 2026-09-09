use super::{
    Expectation, Persistence, UpgradeKind,
    head::{ResponseHead, ValidatedRequestHead, ValidatedResponseHead},
    upgrade_protocols_match,
};
use http::{HeaderValue, Method, StatusCode, header::UPGRADE};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Direction {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Progress {
    Active,
    Stopping,
    Complete,
    Aborted,
}

impl Progress {
    fn settled(self) -> bool {
        matches!(self, Self::Complete | Self::Aborted)
    }
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
    Active,
    Reusable,
    Close,
    Handoff(UpgradeKind),
}

/// Protocol decisions for one exchange, independent of service-future lifetime.
///
/// A head event is not an I/O-completion event. Drivers must acknowledge both
/// directions only after their framing, producer terminal checks, and owned I/O
/// have settled. An abort requests that work stop; it never asserts that buffers
/// have returned. No transport, body allocation, or scheduling lives here.
#[derive(Debug)]
pub(super) struct Exchange {
    request: Progress,
    response: Progress,
    phase: ResponsePhase,
    persistence: Persistence,
    failed: bool,
    method: Method,
    upgrade: Option<UpgradeKind>,
    offered_protocols: Vec<HeaderValue>,
    informational: usize,
    max_informational: usize,
    waiting_continue: bool,
}

impl Exchange {
    pub(super) fn new(request: &ValidatedRequestHead, max_informational: usize) -> Self {
        Self {
            request: Progress::Active,
            response: Progress::Active,
            phase: ResponsePhase::AwaitingHead,
            persistence: request.persistence,
            failed: false,
            method: request.head.method.clone(),
            upgrade: request.upgrade,
            offered_protocols: if request.upgrade == Some(UpgradeKind::Protocol) {
                request.head.headers.get_all(UPGRADE).iter().cloned().collect()
            } else {
                Vec::new()
            },
            informational: 0,
            max_informational,
            waiting_continue: request.expectation == Expectation::Continue,
        }
    }

    /// Validate a peer response in the context of its request. A final response
    /// permits an Expect-gated upload to proceed; it does not cancel that upload.
    pub(super) fn receive_head(
        &mut self,
        head: ResponseHead,
    ) -> Result<(ValidatedResponseHead, HeadAction), crate::Error> {
        let result = self.accept_head(head);
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn accept_head(&mut self, head: ResponseHead) -> Result<(ValidatedResponseHead, HeadAction), crate::Error> {
        if self.failed || self.phase != ResponsePhase::AwaitingHead {
            return Err(crate::Error::new(
                crate::ErrorKind::InvalidMessage,
                "response head after final response",
            ));
        }
        let head = head.validate(&self.method)?;
        if let Some(upgrade) = head.upgrade {
            if self.upgrade != Some(upgrade)
                || (upgrade == UpgradeKind::Protocol
                    && !upgrade_protocols_match(&self.offered_protocols, &head.head.headers))
            {
                return Err(crate::Error::new(
                    crate::ErrorKind::Upgrade,
                    "response did not match the requested upgrade",
                ));
            }
            self.phase = ResponsePhase::Upgrade(upgrade);
            self.waiting_continue = false;
            return Ok((head, HeadAction::Upgrade(upgrade)));
        }
        if head.head.status.is_informational() {
            if self.informational == self.max_informational {
                return Err(crate::Error::new(
                    crate::ErrorKind::Limit,
                    "too many informational responses",
                ));
            }
            self.informational += 1;
            if head.head.status == StatusCode::CONTINUE {
                self.waiting_continue = false;
            }
            return Ok((head, HeadAction::Informational));
        }
        self.phase = ResponsePhase::Final;
        self.waiting_continue = false;
        if head.persistence == Persistence::Close {
            self.persistence = Persistence::Close;
        }
        Ok((head, HeadAction::Final))
    }

    /// Release an Expect wait after a driver's explicit timeout/proceed decision.
    pub(super) fn allow_request_body(&mut self) {
        self.waiting_continue = false;
    }

    pub(super) fn request_body_allowed(&self) -> bool {
        self.request == Progress::Active && !self.waiting_continue
    }

    /// Prevent reuse without pretending that outstanding work has completed.
    pub(super) fn close_after_exchange(&mut self) {
        self.persistence = Persistence::Close;
    }

    pub(super) fn abort(&mut self) {
        self.failed = true;
        for progress in [&mut self.request, &mut self.response] {
            if *progress == Progress::Active {
                *progress = Progress::Stopping;
            }
        }
    }

    /// Acknowledge actual direction completion, including any pending operation.
    /// `clean` requires a complete body and successful terminal producer checks.
    pub(super) fn settle(&mut self, direction: Direction, clean: bool) -> Result<(), crate::Error> {
        let progress = match direction {
            Direction::Request => &mut self.request,
            Direction::Response => &mut self.response,
        };
        if progress.settled()
            || (direction == Direction::Response && clean && self.phase == ResponsePhase::AwaitingHead)
        {
            self.abort();
            return Err(crate::Error::new(
                crate::ErrorKind::Internal,
                "invalid exchange completion transition",
            ));
        }
        *progress = if clean { Progress::Complete } else { Progress::Aborted };
        if !clean {
            self.abort();
        }
        Ok(())
    }

    pub(super) fn outcome(&self) -> Outcome {
        if !self.request.settled() || !self.response.settled() {
            return Outcome::Active;
        }
        if self.failed {
            return Outcome::Close;
        }
        if let ResponsePhase::Upgrade(kind) = self.phase {
            return Outcome::Handoff(kind);
        }
        match self.persistence {
            Persistence::Reusable => Outcome::Reusable,
            Persistence::Close => Outcome::Close,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::h1::{
        config::Config,
        head::{HeadParser, ParseOutcome, RequestRole, ResponseRole},
    };

    fn exchange(wire: &[u8]) -> Exchange {
        let config = Config::default();
        let mut parser = HeadParser::<RequestRole>::request(config.head);
        let ParseOutcome::Complete { head, .. } = parser.parse(wire).unwrap() else {
            panic!("incomplete request")
        };
        Exchange::new(&head.validate().unwrap(), config.max_informational)
    }

    fn response(wire: &[u8]) -> ResponseHead {
        let mut parser = HeadParser::<ResponseRole>::response(Config::default().head);
        let ParseOutcome::Complete { head, .. } = parser.parse(wire).unwrap() else {
            panic!("incomplete response")
        };
        head
    }

    #[test]
    fn early_final_preserves_upload_and_reuse_waits_for_both_directions() {
        for status in ["200 OK", "400 Bad Request", "500 Internal Server Error"] {
            for first in [Direction::Request, Direction::Response] {
                let mut state = exchange(
                    b"POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\nExpect: 100-continue\r\n\r\n",
                );
                assert!(!state.request_body_allowed());
                let (_, action) = state
                    .receive_head(response(
                        format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                    ))
                    .unwrap();
                assert_eq!(action, HeadAction::Final);
                assert!(state.request_body_allowed());
                state.settle(first, true).unwrap();
                assert_eq!(state.outcome(), Outcome::Active);
                let second = if first == Direction::Request {
                    Direction::Response
                } else {
                    Direction::Request
                };
                state.settle(second, true).unwrap();
                assert_eq!(state.outcome(), Outcome::Reusable);
            }
        }
    }

    #[test]
    fn abort_does_not_claim_completion_or_allow_reuse() {
        let mut state = exchange(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n");
        state.abort();
        assert_eq!(state.outcome(), Outcome::Active);
        assert!(!state.request_body_allowed());
        state.settle(Direction::Request, false).unwrap();
        assert_eq!(state.outcome(), Outcome::Active);
        state.settle(Direction::Response, false).unwrap();
        assert_eq!(state.outcome(), Outcome::Close);
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
        assert!(!state.request_body_allowed());
        state.receive_head(response(b"HTTP/1.1 100 Continue\r\n\r\n")).unwrap();
        assert!(state.request_body_allowed());
        for _ in 2..16 {
            state.receive_head(response(early)).unwrap();
        }
        assert_eq!(
            state.receive_head(response(early)).unwrap_err().kind(),
            crate::ErrorKind::Limit
        );
        assert!(!state.request_body_allowed());

        let mut state = exchange(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n");
        state
            .receive_head(response(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
            .unwrap();
        assert_eq!(
            state.receive_head(response(early)).unwrap_err().kind(),
            crate::ErrorKind::InvalidMessage
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
            state.settle(Direction::Response, true).unwrap();
            assert_eq!(state.outcome(), Outcome::Active);
            state.settle(Direction::Request, true).unwrap();
            assert_eq!(state.outcome(), Outcome::Handoff(kind));
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
                crate::ErrorKind::Upgrade
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
            state.settle(Direction::Request, true).unwrap();
            assert_eq!(state.outcome(), Outcome::Active);
            state.settle(Direction::Response, true).unwrap();
            assert_eq!(state.outcome(), Outcome::Close);
        }
    }
}
