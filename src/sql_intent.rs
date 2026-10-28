//! What a SQL string does: read or write, and whether it can leave state in
//! the database session behind.
//!
//! The host decides the required scope (`db:read` / `db:write`) and whether
//! a statement runs in a read-only transaction from [`analyze_statement`],
//! never from the intent a client sends. The kit uses the same analysis to
//! pick the write token, so a module and the host agree.
//!
//! Rules (fail-closed: whatever is not clearly a plain read is a write):
//! - Comments (`--`, nested `/* */`), string literals (`'…'`, `E'…'`,
//!   `$tag$…$tag$`) and quoted identifiers (`"…"`) are skipped, so their
//!   content never counts as a keyword.
//! - Every statement of a multi-statement string is checked (`;` at the top
//!   level); one write makes the whole string a write.
//! - A read starts with `SELECT`, `WITH`, `VALUES`, `TABLE`, `SHOW` or
//!   `EXPLAIN` (optionally behind `(`) and contains none of
//!   `INSERT UPDATE DELETE MERGE INTO COPY` nor a row lock (`FOR UPDATE`,
//!   `FOR SHARE`, …), so data-modifying CTEs (`WITH x AS (UPDATE …)`) and
//!   `SELECT … INTO` are writes.
//! - Calls of functions with side effects or session effects (`set_config`,
//!   `nextval`, advisory locks, large objects, `dblink`, `pg_notify`,
//!   backend signals, …) make a statement a write.
//! - An unterminated comment, literal or identifier, a Unicode-escaped
//!   identifier (`U&"…"`) or anything else the lexer does not understand
//!   counts as a write.
//!
//! Functions a module defines itself can still write from inside a read;
//! the host therefore runs reads in a read-only transaction as well.

use crate::connector::DbConnectorIntent;

/// What a statement may leave behind in the database session after it ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEffect {
    /// Plain DML or a read in its own (implicit) transaction: nothing is
    /// left behind once the statement completed.
    None,
    /// May change session state (`SET`, explicit transactions, temporary
    /// tables, advisory locks, `LISTEN`, cursors, …): the connection must be
    /// reset before anyone else uses it.
    Resettable,
    /// Touches prepared statements or discards session state wholesale
    /// (`PREPARE`, `DEALLOCATE`, `DISCARD`): the connection must not be
    /// reused at all.
    Unrecoverable,
}

/// The result of [`analyze_statement`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementAnalysis {
    /// `Read` only when every statement is a plain read.
    pub intent: DbConnectorIntent,
    /// The strongest session effect of any statement in the string.
    pub session: SessionEffect,
    /// Number of non-empty statements.
    pub statements: usize,
}

impl StatementAnalysis {
    pub fn is_read(&self) -> bool {
        matches!(self.intent, DbConnectorIntent::Read)
    }

    fn write(session: SessionEffect, statements: usize) -> Self {
        Self {
            intent: DbConnectorIntent::Write,
            session,
            statements,
        }
    }
}

/// Words that may start a read.
const READ_STARTS: &[&str] = &["select", "with", "values", "table", "show", "explain"];

/// Words that make a read-starting statement a write wherever they appear:
/// DML (also inside a CTE), `SELECT … INTO`, `COPY`, row locks.
const WRITE_WORDS: &[&str] = &[
    "insert", "update", "delete", "merge", "into", "copy", "share", "truncate", "create", "alter",
    "drop", "grant", "revoke", "lock",
];

/// Statement starts that are DML and leave no session state behind when they
/// run in their own transaction.
const DML_STARTS: &[&str] = &[
    "insert", "update", "delete", "merge", "with", "select", "values", "table",
];

/// Statement starts that make the connection unusable for others.
const UNRECOVERABLE_STARTS: &[&str] = &["prepare", "deallocate", "discard"];

/// Words that, anywhere in a statement, mean session state may survive it.
const SESSION_WORDS: &[&str] = &["temp", "temporary", "listen"];

/// Functions with side effects or session effects. A call (`name(`) makes the
/// statement a write that needs a connection reset afterwards.
const EFFECT_FUNCTIONS: &[&str] = &[
    "set_config",
    "nextval",
    "setval",
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
    "pg_advisory_unlock",
    "pg_advisory_unlock_shared",
    "pg_advisory_unlock_all",
    "pg_advisory_xact_lock",
    "pg_advisory_xact_lock_shared",
    "pg_try_advisory_xact_lock",
    "pg_try_advisory_xact_lock_shared",
    "lo_import",
    "lo_export",
    "lo_create",
    "lo_creat",
    "lo_unlink",
    "lo_put",
    "lo_from_bytea",
    "lo_open",
    "lo_truncate",
    "lowrite",
    "dblink",
    "dblink_exec",
    "dblink_connect",
    "dblink_connect_u",
    "dblink_send_query",
    "dblink_open",
    "pg_notify",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_switch_wal",
    "pg_create_restore_point",
    "pg_logical_emit_message",
    "pg_create_logical_replication_slot",
    "pg_create_physical_replication_slot",
    "pg_drop_replication_slot",
    "pg_stat_reset",
    "pg_stat_reset_shared",
    "pg_stat_reset_single_table_counters",
    "pg_stat_reset_single_function_counters",
    "pg_file_write",
    "pg_file_unlink",
    "pg_file_rename",
    "txid_current",
    "pg_current_xact_id",
];

/// Classifies `sql`. See the module docs for the rules.
pub fn analyze_statement(sql: &str) -> StatementAnalysis {
    let Some(tokens) = lex(sql) else {
        return StatementAnalysis::write(SessionEffect::Unrecoverable, 0);
    };
    let mut intent = DbConnectorIntent::Read;
    let mut session = SessionEffect::None;
    let mut statements = 0usize;
    for statement in tokens.split(|token| matches!(token, Token::Semicolon)) {
        if statement.is_empty() {
            continue;
        }
        statements += 1;
        let (is_read, effect) = analyze_one(statement);
        if !is_read {
            intent = DbConnectorIntent::Write;
        }
        session = strongest(session, effect);
    }
    if statements == 0 {
        // Nothing to run: not a read the host could vouch for.
        return StatementAnalysis::write(SessionEffect::None, 0);
    }
    StatementAnalysis {
        intent,
        session,
        statements,
    }
}

fn strongest(a: SessionEffect, b: SessionEffect) -> SessionEffect {
    match (a, b) {
        (SessionEffect::Unrecoverable, _) | (_, SessionEffect::Unrecoverable) => {
            SessionEffect::Unrecoverable
        }
        (SessionEffect::Resettable, _) | (_, SessionEffect::Resettable) => {
            SessionEffect::Resettable
        }
        _ => SessionEffect::None,
    }
}

/// `(is_read, session effect)` of one statement.
fn analyze_one(tokens: &[Token<'_>]) -> (bool, SessionEffect) {
    let Some(first) = tokens
        .iter()
        .find(|token| !matches!(token, Token::OpenParen))
    else {
        return (false, SessionEffect::None);
    };
    let Token::Word(first) = first else {
        // A statement that starts with a literal, an operator or a quoted
        // identifier is nothing the host can vouch for.
        return (false, SessionEffect::Resettable);
    };
    let first = first.to_ascii_lowercase();
    let first = first.as_str();

    let mut has_write_word = false;
    let mut has_session_word = false;
    let mut calls_effect_function = false;
    let mut suspicious = false;
    for (index, token) in tokens.iter().enumerate() {
        let followed_by_paren = matches!(tokens.get(index + 1), Some(Token::OpenParen));
        match token {
            Token::Word(word) => {
                let lower = word.to_ascii_lowercase();
                if WRITE_WORDS.contains(&lower.as_str()) {
                    has_write_word = true;
                }
                if SESSION_WORDS.contains(&lower.as_str()) {
                    has_session_word = true;
                }
                if followed_by_paren && EFFECT_FUNCTIONS.contains(&lower.as_str()) {
                    calls_effect_function = true;
                }
            }
            Token::QuotedIdent(name) => {
                // Quoted identifiers are case-sensitive: only the exact
                // lower-case name resolves to the built-in function.
                if followed_by_paren && EFFECT_FUNCTIONS.contains(&name.as_str()) {
                    calls_effect_function = true;
                }
            }
            Token::UnicodeIdent => suspicious = true,
            Token::OpenParen | Token::CloseParen | Token::Semicolon | Token::Other => {}
        }
    }

    let session = if UNRECOVERABLE_STARTS.contains(&first) {
        SessionEffect::Unrecoverable
    } else if !DML_STARTS.contains(&first)
        || has_session_word
        || calls_effect_function
        || suspicious
    {
        SessionEffect::Resettable
    } else {
        SessionEffect::None
    };
    let is_read = READ_STARTS.contains(&first)
        && !has_write_word
        && !has_session_word
        && !calls_effect_function
        && !suspicious;
    (is_read, session)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token<'a> {
    /// An unquoted identifier or keyword.
    Word(&'a str),
    /// A quoted identifier, unescaped (`"a""b"` → `a"b`).
    QuotedIdent(String),
    /// `U&"…"`: never trusted.
    UnicodeIdent,
    OpenParen,
    CloseParen,
    Semicolon,
    /// Literals, numbers, operators, parameters.
    Other,
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

/// Splits `sql` into tokens the way the PostgreSQL lexer does for the
/// constructs that matter here. `None` for anything unterminated.
fn lex(sql: &str) -> Option<Vec<Token<'_>>> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let byte = bytes[i];
        match byte {
            b' ' | b'\t' | b'\n' | b'\r' | b'\x0b' | b'\x0c' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                // PostgreSQL ends a line comment at \n *or* \r
                // (scan.l: `non_newline [^\n\r]`).
                while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b'\r' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = skip_block_comment(bytes, i)?;
            }
            b'\'' => {
                i = skip_quoted(bytes, i, b'\'', false)?;
                tokens.push(Token::Other);
            }
            b'"' => {
                let end = skip_quoted(bytes, i, b'"', false)?;
                let inner = sql.get(i + 1..end - 1)?;
                tokens.push(Token::QuotedIdent(inner.replace("\"\"", "\"")));
                i = end;
            }
            b'$' => {
                if let Some(end) = dollar_quote_end(bytes, i)? {
                    tokens.push(Token::Other);
                    i = end;
                } else {
                    // `$1` parameter or a lone `$`.
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                    tokens.push(Token::Other);
                }
            }
            b'(' => {
                tokens.push(Token::OpenParen);
                i += 1;
            }
            b')' => {
                tokens.push(Token::CloseParen);
                i += 1;
            }
            b';' => {
                tokens.push(Token::Semicolon);
                i += 1;
            }
            _ if is_ident_start(byte) => {
                let start = i;
                while i < bytes.len() && is_ident_continue(bytes[i]) {
                    i += 1;
                }
                let word = sql.get(start..i)?;
                match bytes.get(i) {
                    // E'…': backslash escapes. B'…', X'…', N'…': plain.
                    Some(b'\'') if word.eq_ignore_ascii_case("e") => {
                        i = skip_quoted(bytes, i, b'\'', true)?;
                        tokens.push(Token::Other);
                    }
                    Some(b'\'')
                        if word.eq_ignore_ascii_case("b")
                            || word.eq_ignore_ascii_case("x")
                            || word.eq_ignore_ascii_case("n") =>
                    {
                        i = skip_quoted(bytes, i, b'\'', false)?;
                        tokens.push(Token::Other);
                    }
                    // U&'…' (quotes doubled only) and U&"…".
                    Some(b'&')
                        if word.eq_ignore_ascii_case("u")
                            && matches!(bytes.get(i + 1), Some(b'\'' | b'"')) =>
                    {
                        let quote = bytes[i + 1];
                        i = skip_quoted(bytes, i + 1, quote, false)?;
                        tokens.push(if quote == b'"' {
                            Token::UnicodeIdent
                        } else {
                            Token::Other
                        });
                    }
                    _ => tokens.push(Token::Word(word)),
                }
            }
            b'0'..=b'9' => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                    i += 1;
                }
                tokens.push(Token::Other);
            }
            _ => {
                tokens.push(Token::Other);
                i += 1;
            }
        }
    }
    Some(tokens)
}

/// End (exclusive) of a nested block comment starting at `start`.
fn skip_block_comment(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth = depth.checked_sub(1)?;
            i += 2;
            if depth == 0 {
                return Some(i);
            }
        } else {
            i += 1;
        }
    }
    None
}

/// End (exclusive) of a literal or identifier quoted with `quote` that
/// starts at `start`. A doubled quote is part of the content; with
/// `backslash`, `\x` is too.
fn skip_quoted(bytes: &[u8], start: usize, quote: u8, backslash: bool) -> Option<usize> {
    let mut i = start + 1;
    while i < bytes.len() {
        let byte = bytes[i];
        if backslash && byte == b'\\' {
            i += 2;
            continue;
        }
        if byte == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

/// For a `$` at `start`: `Some(Some(end))` for a dollar-quoted string,
/// `Some(None)` when it is no dollar quote (`$1`), `None` when it opens one
/// that never closes.
fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<Option<usize>> {
    let mut i = start + 1;
    if bytes.get(i).is_some_and(u8::is_ascii_digit) {
        return Some(None);
    }
    while i < bytes.len() && bytes[i] != b'$' {
        if !is_ident_continue(bytes[i]) {
            return Some(None);
        }
        i += 1;
    }
    if i >= bytes.len() {
        return Some(None);
    }
    let tag = &bytes[start..=i];
    let body_start = i + 1;
    let mut j = body_start;
    while j + tag.len() <= bytes.len() {
        if &bytes[j..j + tag.len()] == tag {
            return Some(Some(j + tag.len()));
        }
        j += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(sql: &str) -> DbConnectorIntent {
        analyze_statement(sql).intent
    }

    fn is_read(sql: &str) -> bool {
        analyze_statement(sql).is_read()
    }

    #[test]
    fn plain_reads_are_reads() {
        for sql in [
            "SELECT 1",
            "select * from athenos_tickets where id = $1::uuid",
            "  (SELECT a FROM t) UNION (SELECT b FROM u)",
            "WITH x AS (SELECT id FROM t) SELECT * FROM x",
            "WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r",
            "VALUES (1), (2)",
            "TABLE athenos_projects",
            "SHOW search_path",
            "EXPLAIN SELECT 1",
            "SELECT 1; SELECT 2;",
            "SELECT CASE WHEN a THEN 1 ELSE 2 END AS updated_at FROM t",
            "SELECT count(*) FROM athenos_ticket_comments c WHERE c.deleted_at IS NULL",
            "SELECT id FROM t WHERE title ILIKE '%' || $1 || '%'",
        ] {
            assert!(is_read(sql), "{sql}");
        }
    }

    #[test]
    fn data_modifying_ctes_are_writes() {
        for sql in [
            "WITH x AS (UPDATE t SET a = 1 RETURNING id) SELECT * FROM x",
            "WITH x AS (INSERT INTO t (a) VALUES (1) RETURNING id) SELECT id FROM x",
            "WITH x AS (DELETE FROM t WHERE a = 1 RETURNING id) SELECT count(*) FROM x",
            "with x as (merge into t using u on t.id = u.id when matched then do nothing) select 1",
            "WITH y AS (SELECT 1) UPDATE t SET a = 1 RETURNING *",
            "WITH y AS (SELECT 1) INSERT INTO t SELECT * FROM y",
        ] {
            assert_eq!(intent(sql), DbConnectorIntent::Write, "{sql}");
        }
    }

    #[test]
    fn locks_into_and_effect_functions_are_writes() {
        for sql in [
            "SELECT * FROM t FOR UPDATE",
            "SELECT * FROM t FOR NO KEY UPDATE SKIP LOCKED",
            "SELECT * FROM t FOR SHARE",
            "SELECT * FROM t FOR KEY SHARE",
            "SELECT * INTO backup FROM t",
            "SELECT * INTO TEMP x FROM t",
            "SELECT set_config('search_path', 'evil', false)",
            "SELECT pg_catalog.set_config('search_path', 'evil', false)",
            "SELECT \"set_config\"('search_path', 'evil', false)",
            "SELECT nextval('athenos_seq')",
            "SELECT pg_advisory_lock(42)",
            "SELECT dblink_exec('dbname=x', 'DELETE FROM t')",
            "SELECT pg_terminate_backend(123)",
            "SELECT U&\"\\0073et_config\"('a', 'b', false)",
            "EXPLAIN ANALYZE UPDATE t SET a = 1",
        ] {
            assert_eq!(intent(sql), DbConnectorIntent::Write, "{sql}");
        }
    }

    #[test]
    fn other_statements_are_writes() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "update t set a = 1",
            "DELETE FROM t",
            "MERGE INTO t USING u ON true WHEN MATCHED THEN DELETE",
            "COPY t FROM STDIN",
            "CREATE TABLE x (a int)",
            "TRUNCATE t",
            "SET search_path = evil",
            "BEGIN",
            "DO $$ BEGIN PERFORM 1; END $$",
            "CALL p()",
            "LISTEN ch",
            "PREPARE p AS SELECT 1",
            "SELECT 1; DELETE FROM t",
            "SELECT 1; COMMIT; UPDATE t SET a = 1",
            "describe t",
            "",
            "   ",
            ";;",
        ] {
            assert_eq!(intent(sql), DbConnectorIntent::Write, "{sql:?}");
        }
    }

    #[test]
    fn literals_comments_and_identifiers_do_not_confuse_it() {
        // Keywords inside literals, comments and quoted identifiers are no
        // keywords: these are reads.
        for sql in [
            "SELECT 'UPDATE t SET a = 1; DELETE FROM t' AS text",
            "SELECT 'it''s; UPDATE' FROM t",
            "SELECT E'\\'; UPDATE t SET a = 1; --' AS x",
            "SELECT $$; DELETE FROM t; $$ AS body",
            "SELECT $tag$ UPDATE $other$ $tag$",
            "SELECT \"update\", \"insert\" FROM t",
            "SELECT 1 -- ; DELETE FROM t",
            "SELECT /* ; DROP TABLE t; /* nested */ still comment */ 1",
            "SELECT a$b FROM t WHERE x = $1",
        ] {
            assert!(is_read(sql), "{sql}");
        }
        // Text a lexer could wrongly swallow into a literal stays visible:
        // a plain '…' has no backslash escapes (standard_conforming_strings).
        assert_eq!(
            intent("SELECT 'a\\'; UPDATE t SET a = 1; --'"),
            DbConnectorIntent::Write
        );
        // A line comment ends at \r as well as \n, as in PostgreSQL: what
        // follows a lone \r is code.
        for sql in [
            "SELECT 1 --x\r; COMMIT; DELETE FROM t",
            "SELECT 1 -- /*\r; DELETE FROM t",
            "SELECT 1 -- c\r; SET ROLE other",
        ] {
            assert_eq!(intent(sql), DbConnectorIntent::Write, "{sql:?}");
        }
        assert!(is_read("SELECT 1 -- comment\r\n"));
        // Unterminated constructs are writes.
        for sql in [
            "SELECT 'unterminated",
            "SELECT \"unterminated",
            "SELECT /* unterminated",
            "SELECT $x$ unterminated",
        ] {
            assert_eq!(intent(sql), DbConnectorIntent::Write, "{sql}");
        }
    }

    #[test]
    fn session_effects() {
        assert_eq!(analyze_statement("SELECT 1").session, SessionEffect::None);
        assert_eq!(
            analyze_statement("UPDATE t SET a = 1 WHERE id = $1").session,
            SessionEffect::None
        );
        assert_eq!(
            analyze_statement("WITH x AS (DELETE FROM t RETURNING id) SELECT * FROM x").session,
            SessionEffect::None
        );
        for sql in [
            "SET search_path = x",
            "BEGIN",
            "CREATE TEMP TABLE x (a int)",
            "SELECT * INTO TEMP x FROM t",
            "SELECT set_config('a.b', 'c', false)",
            "SELECT pg_advisory_lock(1)",
            "LISTEN ch",
            "DECLARE c CURSOR WITH HOLD FOR SELECT 1",
            "INSERT INTO t VALUES (1); SET x = 1",
        ] {
            assert_eq!(
                analyze_statement(sql).session,
                SessionEffect::Resettable,
                "{sql}"
            );
        }
        for sql in ["PREPARE p AS SELECT 1", "DEALLOCATE ALL", "DISCARD ALL"] {
            assert_eq!(
                analyze_statement(sql).session,
                SessionEffect::Unrecoverable,
                "{sql}"
            );
        }
    }

    #[test]
    fn detect_uses_the_analysis() {
        assert!(matches!(
            DbConnectorIntent::detect(
                "WITH x AS (UPDATE t SET a = 1 RETURNING id) SELECT * FROM x"
            ),
            DbConnectorIntent::Write
        ));
        assert!(matches!(
            DbConnectorIntent::detect("WITH x AS (SELECT 1) SELECT * FROM x"),
            DbConnectorIntent::Read
        ));
    }
}
