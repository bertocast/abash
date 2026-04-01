use abash_core::SandboxError;
use time::{OffsetDateTime, UtcOffset};

pub(crate) fn execute(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    let now = current_time()?;

    let rendered = match args {
        [] => render_default(now),
        [format] if format.starts_with('+') => render_format(now, &format[1..])?,
        [flag] if flag.starts_with('-') => {
            return Err(SandboxError::InvalidRequest(format!(
                "date flag is not supported: {flag}"
            )))
        }
        [_] => {
            return Err(SandboxError::InvalidRequest(
                "date supports only optional +FORMAT".to_string(),
            ))
        }
        _ => {
            return Err(SandboxError::InvalidRequest(
                "date supports only optional +FORMAT".to_string(),
            ))
        }
    };

    Ok(format!("{rendered}\n").into_bytes())
}

fn current_time() -> Result<OffsetDateTime, SandboxError> {
    let now = OffsetDateTime::now_utc();
    let offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    now.to_offset(offset)
        .replace_nanosecond(0)
        .map_err(|error| {
            SandboxError::BackendFailure(format!("failed to build current time: {error}"))
        })
}

fn render_default(now: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        render_offset(now.offset()),
    )
}

fn render_format(now: OffsetDateTime, format: &str) -> Result<String, SandboxError> {
    let mut output = String::new();
    let chars = format.chars().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        if chars[index] != '%' {
            output.push(chars[index]);
            index += 1;
            continue;
        }

        let Some(specifier) = chars.get(index + 1) else {
            return Err(SandboxError::InvalidRequest(
                "date format string cannot end with %".to_string(),
            ));
        };
        match specifier {
            '%' => output.push('%'),
            'Y' => output.push_str(&format!("{:04}", now.year())),
            'm' => output.push_str(&format!("{:02}", u8::from(now.month()))),
            'd' => output.push_str(&format!("{:02}", now.day())),
            'H' => output.push_str(&format!("{:02}", now.hour())),
            'M' => output.push_str(&format!("{:02}", now.minute())),
            'S' => output.push_str(&format!("{:02}", now.second())),
            'F' => output.push_str(&format!(
                "{:04}-{:02}-{:02}",
                now.year(),
                u8::from(now.month()),
                now.day()
            )),
            'T' => output.push_str(&format!(
                "{:02}:{:02}:{:02}",
                now.hour(),
                now.minute(),
                now.second()
            )),
            's' => output.push_str(&now.unix_timestamp().to_string()),
            'z' => output.push_str(&render_offset_compact(now.offset())),
            _ => {
                return Err(SandboxError::InvalidRequest(format!(
                    "date format specifier is not supported: %{specifier}"
                )))
            }
        }
        index += 2;
    }

    Ok(output)
}

fn render_offset(offset: UtcOffset) -> String {
    let sign = if offset.is_negative() { '-' } else { '+' };
    let hours = offset.whole_hours().unsigned_abs();
    let minutes = (offset.whole_minutes().unsigned_abs()) % 60;
    format!("{sign}{hours:02}:{minutes:02}")
}

fn render_offset_compact(offset: UtcOffset) -> String {
    let sign = if offset.is_negative() { '-' } else { '+' };
    let hours = offset.whole_hours().unsigned_abs();
    let minutes = (offset.whole_minutes().unsigned_abs()) % 60;
    format!("{sign}{hours:02}{minutes:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_time() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_735_498_096)
            .unwrap()
            .to_offset(UtcOffset::from_hms(1, 0, 0).unwrap())
    }

    #[test]
    fn default_render_is_iso_like() {
        assert_eq!(render_default(sample_time()), "2024-12-29T19:48:16+01:00");
    }

    #[test]
    fn supports_common_format_tokens() {
        let rendered = render_format(sample_time(), "%F %T %z").unwrap();
        assert_eq!(rendered, "2024-12-29 19:48:16 +0100");
    }
}
