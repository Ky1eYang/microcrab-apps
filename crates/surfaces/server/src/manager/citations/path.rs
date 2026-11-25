//! Explicit CommonMark destinations. URL decoding happens exactly once here;
//! the HTTP query decoder is a separate transport layer, never a path normalizer.
use pulldown_cmark::{Event, Options, Parser, Tag};

pub(super) const MAX_MARKDOWN_BYTES: usize = 1024 * 1024;
pub(super) const MAX_HREF_BYTES: usize = 4096;

pub(in crate::manager) fn path(href: &str) -> Option<String> {
    if href.is_empty() || href.len() > MAX_HREF_BYTES || href.starts_with('#') {
        return None;
    }
    let raw = href.split(['?', '#']).next()?;
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let digits = bytes.get(index + 1..index + 3)?;
            let hex = std::str::from_utf8(digits).ok()?;
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let decoded = String::from_utf8(decoded).ok()?;
    // Reject double encoding, controls, credentials, device paths and ADS.
    if decoded.chars().any(|c| c.is_control()) || decoded.contains(['%', '@', '?', '#']) {
        return None;
    }
    let mut value = decoded.replace('\\', "/");
    // A trailing :123 is a line locator, never an alternate stream.
    if let Some((base, line)) = value.rsplit_once(':')
        && line.starts_with(|c: char| ('1'..='9').contains(&c))
        && line.bytes().all(|c| c.is_ascii_digit())
    {
        value = base.to_owned();
    }
    let drive = value.as_bytes().get(1) == Some(&b':')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
        && value.as_bytes().get(2) == Some(&b'/');
    if value.contains(':') && (!drive || value[2..].contains(':')) {
        return None;
    }
    let unc = value.starts_with("//");
    // Only backslash UNC is accepted. //web/path stays an external URL.
    if unc && !raw.starts_with("\\\\") {
        return None;
    }
    let mut parts = Vec::new();
    for part in value.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." || part.ends_with(['.', ' ']) || part.contains(['<', '>', '"', '|', '*']) {
            return None;
        }
        parts.push(part);
    }
    if parts.is_empty() || (drive && parts.len() < 2) || (unc && parts.len() < 3) {
        return None;
    }
    let prefix = if unc {
        "//"
    } else if value.starts_with('/') {
        "/"
    } else {
        ""
    };
    Some(format!("{prefix}{}", parts.join("/")))
}

pub(super) fn eligible(markdown: &str, href: &str) -> Option<String> {
    eligible_destination(markdown, href, false)
}

pub(in crate::manager) fn eligible_image(markdown: &str, href: &str) -> Option<String> {
    eligible_destination(markdown, href, true)
}

fn eligible_destination(markdown: &str, href: &str, image_only: bool) -> Option<String> {
    if markdown.len() > MAX_MARKDOWN_BYTES {
        return None;
    }
    let requested = path(href)?;
    Parser::new_ext(
        markdown,
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
    )
    .any(|event| match event {
        Event::Start(Tag::Image { dest_url, .. }) => path(&dest_url).as_ref() == Some(&requested),
        Event::Start(Tag::Link { dest_url, .. }) if !image_only => {
            path(&dest_url).as_ref() == Some(&requested)
        }
        _ => false,
    })
    .then_some(requested)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn grammar_and_single_decoding() {
        for (href, expected) in [
            ("./docs/a.md#L123", "docs/a.md"),
            ("docs/a.md:123?download=1", "docs/a.md"),
            ("/tmp/a%20b.png", "/tmp/a b.png"),
            ("C:/work/a.md:12", "C:/work/a.md"),
            (r"C:\work\a.md", "C:/work/a.md"),
            (r"\\server\share\a.md", "//server/share/a.md"),
        ] {
            assert_eq!(path(href).as_deref(), Some(expected), "{href}");
        }
        for href in [
            "",
            "#L12",
            "https://site/a",
            "mailto:a",
            "file:///tmp/a",
            "//site/a",
            "../secret",
            "a/../b",
            "a/%2e%2e/b",
            "%252e%252e/a",
            "a%00b",
            "C:relative",
            "a:stream",
            r"\\?\C:\a",
            "https://user:pass@site/a",
        ] {
            assert!(path(href).is_none(), "{href}");
        }
    }
    #[test]
    fn only_ast_links_and_images_grant_references() {
        assert!(eligible("[read](./a.md#L12)", "a.md").is_some());
        assert!(eligible("![image](a.png)", "a.png").is_some());
        assert!(eligible("[read][r]\n\n[r]: a.md", "a.md").is_some());
        for text in [
            "a.md",
            "`[read](a.md)`",
            "```md\n[read](a.md)\n```",
            "<a href=\"a.md\">read</a>",
            "[read](https://site/a.md)",
        ] {
            assert!(eligible(text, "a.md").is_none(), "{text}");
        }
    }
}
