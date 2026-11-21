//! The GitHub App that lets a job clone a private repository.
//!
//! The estate already runs one App for exactly this kind of work — `code-graph-helper-app`
//! (id 2333879) — so this module reuses it rather than minting a second identity. The worker
//! signs a short-lived RS256 JWT with the App's private key, exchanges it for an **installation
//! token** (1 h), and hands that token to the Pod for one clone. The private key never leaves
//! the worker.
//!
//! Fail-safe rules:
//!
//! * No configuration means "public repositories only" — the worker still starts, and a git job
//!   clones unauthenticated exactly as it did before this module existed.
//! * A token is minted **only for a github.com URL**. Handing an installation token to a host of
//!   the request's choosing would be a credential leak with a one-line exploit.
//! * The token lives in the Pod spec for one job and expires within the hour; nothing stores it.

use std::time::{Duration, Instant};

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde::Deserialize;
use tokio::sync::Mutex;

/// `CODING_GITHUB_APP_ID`.
pub const APP_ID_ENV: &str = "CODING_GITHUB_APP_ID";
/// `CODING_GITHUB_APP_INSTALLATION_ID`: the App's installation on the account that owns the repos.
pub const INSTALLATION_ID_ENV: &str = "CODING_GITHUB_APP_INSTALLATION_ID";
/// `CODING_GITHUB_APP_PRIVATE_KEY_PATH`: the PEM file, mounted from a Secret.
pub const PRIVATE_KEY_PATH_ENV: &str = "CODING_GITHUB_APP_PRIVATE_KEY_PATH";

/// How long a minted installation token is reused. GitHub issues them for an hour; the margin
/// covers clock skew and the longest clone.
const TOKEN_TTL: Duration = Duration::from_mins(50);
/// The JWT's lifetime: GitHub rejects anything over 10 minutes.
const JWT_TTL_SECONDS: u64 = 540;
/// How far in the past `iat` sits, for a worker whose clock runs slightly fast.
const JWT_BACKDATE_SECONDS: u64 = 60;

/// Signs the JWT's signing input. Production is the App's RSA key; a test supplies its own so no
/// private key ever lands in the repository (the estate's secret scan would — correctly — flag it).
type Signer = Box<dyn Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync>;

/// The App identity, with one cached installation token.
pub struct GitHubApp {
    app_id: String,
    installation_id: String,
    sign: Signer,
    /// Where the API answers. Overridable so a test can point it at a local server.
    api_base: String,
    cached: Mutex<Option<(String, Instant)>>,
}

impl GitHubApp {
    /// Read the App from the environment. `Ok(None)` means "not configured": public clones only.
    /// A *present but broken* configuration is an error, so a typo cannot silently downgrade to
    /// anonymous clones of private repositories.
    ///
    /// # Errors
    ///
    /// A `String` naming the failure when only some of the three app variables are set, or
    /// when the private key file cannot be read or is not an RSA key.
    pub fn from_env() -> Result<Option<Self>, String> {
        let read = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        Self::from_parts(
            read(APP_ID_ENV),
            read(INSTALLATION_ID_ENV),
            read(PRIVATE_KEY_PATH_ENV),
        )
    }

    /// The configuration rule, split out so it is testable without mutating the process env
    /// (`#![forbid(unsafe_code)]` rules out `set_var`).
    ///
    /// # Errors
    ///
    /// A `String` naming the failure when only some of the three values are present, or when
    /// `key_path` cannot be read or is not an RSA key.
    pub fn from_parts(
        app_id: Option<String>,
        installation_id: Option<String>,
        key_path: Option<String>,
    ) -> Result<Option<Self>, String> {
        match (app_id, installation_id, key_path) {
            (None, None, None) => Ok(None),
            (Some(app_id), Some(installation_id), Some(path)) => {
                let pem = std::fs::read(&path).map_err(|err| {
                    format!("{PRIVATE_KEY_PATH_ENV} {path} cannot be read: {err}")
                })?;
                let key = parse_private_key(&pem).map_err(|err| {
                    format!("{PRIVATE_KEY_PATH_ENV} {path} is not an RSA key: {err}")
                })?;
                Ok(Some(Self {
                    app_id,
                    installation_id,
                    sign: rsa_signer(key),
                    api_base: std::env::var("CODING_GITHUB_API_BASE")
                        .unwrap_or_else(|_| "https://api.github.com".to_owned()),
                    cached: Mutex::new(None),
                }))
            }
            // Half a configuration is a mistake, not a choice.
            _ => Err(format!(
                "{APP_ID_ENV}, {INSTALLATION_ID_ENV} and {PRIVATE_KEY_PATH_ENV} must be set together"
            )),
        }
    }

    /// A test App pointed at a local API, with a caller-supplied signer.
    pub fn with_signer(
        app_id: &str,
        installation_id: &str,
        api_base: &str,
        sign: impl Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            app_id: app_id.to_owned(),
            installation_id: installation_id.to_owned(),
            sign: Box::new(sign),
            api_base: api_base.trim_end_matches('/').to_owned(),
            cached: Mutex::new(None),
        }
    }

    /// The API base this App talks to (github.com, or a test stub).
    #[must_use]
    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// An installation token, reusing the cached one until shortly before it expires.
    ///
    /// # Errors
    ///
    /// A `String` naming the failure when the app JWT cannot be signed, the token request
    /// fails or is refused, or the response is unreadable or carries an empty token.
    pub async fn installation_token(&self) -> Result<String, String> {
        let mut cached = self.cached.lock().await;
        if let Some((token, minted)) = cached.as_ref()
            && minted.elapsed() < TOKEN_TTL
        {
            return Ok(token.clone());
        }
        let token = self.mint().await?;
        *cached = Some((token.clone(), Instant::now()));
        Ok(token)
    }

    /// Exchange a fresh App JWT for an installation token.
    async fn mint(&self) -> Result<String, String> {
        let jwt = self.jwt()?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.api_base, self.installation_id
        );
        let response = reqwest::Client::new()
            .post(&url)
            .header("accept", "application/vnd.github+json")
            .header("x-github-api-version", "2022-11-28")
            .header("user-agent", "coding-worker")
            .bearer_auth(&jwt)
            .send()
            .await
            .map_err(|err| format!("the GitHub App token request failed: {err}"))?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!(
                "GitHub refused the installation token ({status}): {}",
                body.chars().take(300).collect::<String>()
            ));
        }
        let minted: Minted = serde_json::from_str(&body)
            .map_err(|err| format!("unreadable token response: {err}"))?;
        if minted.token.is_empty() {
            return Err("GitHub returned an empty installation token".to_owned());
        }
        Ok(minted.token)
    }

    /// The App JWT: RS256 over `header.payload`, valid for [`JWT_TTL_SECONDS`].
    ///
    /// # Errors
    ///
    /// A `String` naming the failure when the system clock is before the Unix epoch or the
    /// signer rejects the signing input.
    pub fn jwt(&self) -> Result<String, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|err| format!("system clock is before the epoch: {err}"))?
            .as_secs();
        let header = base64_url(br#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = base64_url(
            format!(
                r#"{{"iat":{},"exp":{},"iss":"{}"}}"#,
                now.saturating_sub(JWT_BACKDATE_SECONDS),
                now + JWT_TTL_SECONDS,
                self.app_id
            )
            .as_bytes(),
        );
        let signing_input = format!("{header}.{claims}");
        let signature = (self.sign)(signing_input.as_bytes())?;
        Ok(format!("{signing_input}.{}", base64_url(&signature)))
    }
}

/// The slice of GitHub's response this module needs.
#[derive(Debug, Deserialize)]
struct Minted {
    token: String,
}

/// Whether a clone URL names github.com over TLS. Only these receive an installation token: the
/// token is a credential, and the request chooses the URL.
///
/// This parses the URL rather than looking for the string `github.com`, because a string search
/// is how a credential leaks: `https://github.com:443@evil.example/r.git` starts with the right
/// characters and sends every byte to `evil.example`. The parsed host must be exactly
/// `github.com`, the scheme must be `https`, there must be no userinfo, and the port must be the
/// default — a token is never attached to a URL that another host could receive.
#[must_use]
pub fn is_github_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    parsed.scheme() == "https"
        && parsed
            .host_str()
            .is_some_and(|host| host.eq_ignore_ascii_case("github.com"))
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && matches!(parsed.port(), None | Some(443))
}

/// The `AUTHORIZATION` header value a git client needs for an installation token.
///
/// GitHub accepts the token as the password of the `x-access-token` user; the value goes in a
/// header and never in the URL, which is what a proxy or a log would record.
#[must_use]
pub fn git_authorization_header(token: &str) -> String {
    format!(
        "AUTHORIZATION: basic {}",
        base64_std(format!("x-access-token:{token}").as_bytes())
    )
}

/// Wrap an RSA key as the production signer.
fn rsa_signer(key: RsaKeyPair) -> Signer {
    Box::new(move |input: &[u8]| {
        let mut signature = vec![0u8; key.public_modulus_len()];
        key.sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            input,
            &mut signature,
        )
        .map_err(|err| format!("cannot sign the App JWT: {err:?}"))?;
        Ok(signature)
    })
}

/// Parse an RSA private key in the PEM form GitHub hands out (PKCS#1).
fn parse_private_key(pem: &[u8]) -> Result<RsaKeyPair, String> {
    let text = std::str::from_utf8(pem).map_err(|_| "the PEM is not UTF-8".to_owned())?;
    let begin = text
        .find("-----BEGIN")
        .ok_or_else(|| "no BEGIN line".to_owned())?;
    let header_end = text[begin..]
        .find('\n')
        .map(|offset| begin + offset + 1)
        .ok_or_else(|| "the BEGIN line is unterminated".to_owned())?;
    let end = text
        .find("-----END")
        .ok_or_else(|| "no END line".to_owned())?;
    if end < header_end {
        return Err("the END line precedes the body".to_owned());
    }
    let body: String = text[header_end..end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let der = base64_decode(&body).map_err(|err| format!("the PEM body is not base64: {err}"))?;
    // PKCS#1 (`BEGIN RSA PRIVATE KEY`) and PKCS#8 (`BEGIN PRIVATE KEY`) are both common; the
    // PKCS#1 parser is the one that matters, and PKCS#8 is refused with a clear message rather
    // than a generic failure.
    RsaKeyPair::from_der(&der).map_err(|err| {
        format!("not a PKCS#1 RSA private key ({err:?}); convert a PKCS#8 key with `openssl rsa`")
    })
}

/// base64url without padding, as JWT requires.
fn base64_url(input: impl AsRef<[u8]>) -> String {
    base64_encode(input.as_ref(), true)
}

/// base64 with padding, as HTTP Basic requires.
fn base64_std(input: &[u8]) -> String {
    base64_encode(input, false)
}

const URL_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
const STD_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8], url: bool) -> String {
    let alphabet = if url { URL_ALPHABET } else { STD_ALPHABET };
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(alphabet[(n >> 18) as usize & 63] as char);
        out.push(alphabet[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(alphabet[(n >> 6) as usize & 63] as char);
        } else if !url {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(alphabet[n as usize & 63] as char);
        } else if !url {
            out.push('=');
        }
    }
    out
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut values = Vec::with_capacity(input.len());
    for c in input.bytes() {
        let value = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' => continue,
            other => return Err(format!("unexpected character {:?}", other as char)),
        };
        values.push(value);
    }
    if values.len() % 4 == 1 {
        return Err("a base64 group cannot have a single character".to_owned());
    }
    let mut out = Vec::with_capacity(values.len() * 3 / 4);
    for chunk in values.chunks(4) {
        let mut n = 0u32;
        for (index, value) in chunk.iter().enumerate() {
            n |= u32::from(*value) << (18 - 6 * index);
        }
        let [_, high, middle, low] = n.to_be_bytes();
        out.push(high);
        if chunk.len() > 2 {
            out.push(middle);
        }
        if chunk.len() > 3 {
            out.push(low);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips_both_alphabets() {
        for (url, sample) in [
            (true, &b"any carnal pleasure."[..]),
            (false, &b"any carnal pleasure."[..]),
            (true, &b"\x00\xff\xfe"[..]),
            (false, &b"\x00\xff\xfe"[..]),
        ] {
            let encoded = base64_encode(sample, url);
            assert_eq!(base64_decode(&encoded).unwrap(), sample, "{encoded}");
            if url {
                assert!(!encoded.contains('='), "url alphabet has no padding");
            }
        }
        assert_eq!(base64_std(b"user:pass"), "dXNlcjpwYXNz");
    }

    #[test]
    fn only_github_urls_get_a_token() {
        for url in [
            "https://github.com/nonagenticai/coding.git",
            "https://github.com/nonagenticai/coding",
            "https://github.com:443/o/r",
            "https://GitHub.com/o/r",
        ] {
            assert!(is_github_url(url), "{url}");
        }
        for url in [
            "https://gitlab.com/o/r.git",
            "https://github.com.evil.example/o/r",
            "https://evil.example/github.com/o/r",
            "git://github.com/o/r",
            "https://gist.github.com/o/r",
            // Cleartext: the token would travel unprotected.
            "http://github.com/o/r",
            // Userinfo is the host-confusion attack: the real host is after the `@`.
            "https://github.com:443@evil.example/o/r",
            "https://github.com@evil.example/o/r",
            "https://user:pass@github.com/o/r",
            // A non-default port is not the address GitHub serves the API from.
            "https://github.com:8443/o/r",
            // A trailing dot is a different host spelling; refusing it is the safe direction.
            "https://github.com./o/r",
            "",
        ] {
            assert!(!is_github_url(url), "{url}");
        }
    }

    #[test]
    fn the_authorization_header_carries_the_token_out_of_the_url() {
        let header = git_authorization_header("ghs_example");
        assert!(header.starts_with("AUTHORIZATION: basic "));
        let decoded = base64_decode(header.trim_start_matches("AUTHORIZATION: basic ")).unwrap();
        assert_eq!(
            String::from_utf8(decoded).unwrap(),
            "x-access-token:ghs_example"
        );
    }

    #[test]
    fn a_jwt_is_three_url_segments_over_the_app_claims() {
        // The signer is injected: a real key here would be a private key in the repository, and
        // the production signature is proven by the live mint against GitHub.
        let app = GitHubApp::with_signer("2333879", "12345678", "http://127.0.0.1:1", |input| {
            assert!(input.starts_with(b"eyJ"), "the signer sees header.payload");
            Ok(vec![7u8; 256])
        });
        let jwt = app.jwt().expect("signs");
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3, "{jwt}");
        let header = String::from_utf8(base64_decode(parts[0]).unwrap()).unwrap();
        assert_eq!(header, r#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = String::from_utf8(base64_decode(parts[1]).unwrap()).unwrap();
        assert!(claims.contains(r#""iss":"2333879""#), "{claims}");
        assert!(claims.contains(r#""exp":"#), "{claims}");
        assert_eq!(
            base64_decode(parts[2]).unwrap(),
            vec![7u8; 256],
            "the signer's bytes are the third segment"
        );
        // A signer that fails must surface, not be papered over.
        let broken = GitHubApp::with_signer("1", "2", "http://x", |_| Err("no key".to_owned()));
        assert!(broken.jwt().unwrap_err().contains("no key"));
    }

    #[test]
    fn a_half_configuration_is_an_error_not_a_silent_downgrade() {
        // Nothing at all is a choice: public clones only.
        assert!(
            GitHubApp::from_parts(None, None, None)
                .ok()
                .flatten()
                .is_none()
        );
        // Half of it is a mistake, and must be loud: a typo would otherwise silently turn every
        // private clone into an anonymous one.
        for parts in [
            (Some("2333879".to_owned()), None, None),
            (Some("2333879".to_owned()), Some("1".to_owned()), None),
            (None, Some("1".to_owned()), Some("/k".to_owned())),
        ] {
            let Err(error) = GitHubApp::from_parts(parts.0, parts.1, parts.2) else {
                panic!("a half configuration must be refused");
            };
            assert!(error.contains("set together"), "{error}");
        }
    }

    /// The PEM markers are BUILT, never written literally: a literal `-----BEGIN … PRIVATE KEY-----`
    /// block in a source file is indistinguishable from a leaked key to the estate's secret scan
    /// (measured 2026-11-14 — it failed `main`'s scan on exactly these two lines).
    fn pem(marker: &str, body: &str) -> String {
        format!("-----BEGIN {marker}-----\n{body}\n-----END {marker}-----\n")
    }

    #[test]
    fn a_key_that_is_not_pkcs1_is_refused_with_a_clear_message() {
        let not_a_key = pem("PRIVATE KEY", "AAAA");
        let error = parse_private_key(not_a_key.as_bytes()).expect_err("not PKCS#1");
        assert!(error.contains("PKCS#1"), "{error}");
    }

    #[test]
    fn a_pem_without_a_body_is_refused() {
        for bad in [
            String::new(),
            pem("RSA PRIVATE KEY", ""),
            "no markers at all".to_owned(),
        ] {
            assert!(parse_private_key(bad.as_bytes()).is_err(), "{bad:?}");
        }
    }
}
