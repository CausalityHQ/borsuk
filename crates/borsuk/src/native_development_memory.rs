//! Caller memory admission for the native development runners.
use std::error::Error;

fn parse_limit(value: Option<&str>) -> Result<u64, &'static str> {
    value
        .unwrap_or("1073741824")
        .parse::<u64>()
        .ok()
        .filter(|&bytes| bytes != 0)
        .ok_or("BORSUK_NATIVE_MEMORY_BYTES must be a positive u64")
}

pub(crate) fn memory_limit() -> Result<u64, Box<dyn Error>> {
    let value = match std::env::var("BORSUK_NATIVE_MEMORY_BYTES") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    parse_limit(value.as_deref()).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_admission_preserves_default_and_rejects_invalid_bounds() {
        assert_eq!(parse_limit(None), Ok(1_073_741_824));
        assert_eq!(parse_limit(Some("1342177280")), Ok(1_342_177_280));
        for invalid in ["0", "", "-1", "garbage", "18446744073709551616"] {
            assert!(parse_limit(Some(invalid)).is_err());
        }
    }
}
