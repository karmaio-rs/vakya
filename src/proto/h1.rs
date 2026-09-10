mod syntax;
use syntax::trim_ows;

pub(super) mod bridge;
pub(crate) mod config;
pub(super) mod decode;
pub(super) mod encode;
pub(super) mod exchange;
pub(super) mod head;
mod inline;
#[cfg(feature = "server")]
pub(crate) mod server;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BodyMode {
    None,
    Fixed(u64),
    Chunked,
    UntilEof,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Persistence {
    Reusable,
    Close,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UpgradeKind {
    Protocol,
    Tunnel,
}

pub(super) fn upgrade_protocols_match(offered: &[http::HeaderValue], selected: &http::HeaderMap) -> bool {
    let mut selected_any = false;
    for value in selected.get_all(http::header::UPGRADE) {
        for selected in value.as_bytes().split(|byte| *byte == b',').map(trim_ows) {
            if selected.is_empty() {
                return false;
            }
            selected_any = true;
            let was_offered = offered.iter().any(|value| {
                value
                    .as_bytes()
                    .split(|byte| *byte == b',')
                    .map(trim_ows)
                    .any(|offered| offered.eq_ignore_ascii_case(selected))
            });
            if !was_offered {
                return false;
            }
        }
    }
    selected_any
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TargetForm {
    Origin,
    Absolute,
    Authority,
    Asterisk,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Expectation {
    None,
    Continue,
}
