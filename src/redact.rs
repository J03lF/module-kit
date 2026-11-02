//! `Debug` output for structs that hold tokens: the field is listed, its
//! value is not, so a `{:?}` in a log line cannot leak a bearer token.

use std::collections::HashMap;
use std::fmt;

pub(crate) struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Header names whose values carry credentials; compared case-insensitively
/// because HTTP header names are.
pub(crate) fn is_sensitive_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
    ) || name.contains("token")
}

/// A header map whose credential values print as `<redacted>`; the names
/// stay visible so a log line still shows which headers were sent.
pub(crate) struct RedactedHeaders<'a>(pub(crate) &'a HashMap<String, String>);

impl fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (name, value) in self.0 {
            if is_sensitive_header(name) {
                map.entry(name, &Redacted);
            } else {
                map.entry(name, value);
            }
        }
        map.finish()
    }
}
