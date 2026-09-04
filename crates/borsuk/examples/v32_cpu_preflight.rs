//! Corpus-free, claim-ineligible V32 CPU rejection preflight.

use std::{collections::BTreeMap, io::Write as _};

use borsuk::{V32CpuPreflightMode, run_v32_cpu_preflight};

fn parse_args<I, S>(args: I) -> Result<(V32CpuPreflightMode, usize), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values = args.into_iter().collect::<Vec<_>>();
    if values.len() % 2 != 0 {
        return Err("V32 CPU preflight arguments differ".to_owned());
    }
    let mut parsed = BTreeMap::new();
    for [flag, value] in values.as_chunks::<2>().0 {
        let key = flag.as_ref();
        let value = value.as_ref();
        if !matches!(key, "--mode" | "--leaf-beam")
            || parsed.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err("V32 CPU preflight arguments differ".to_owned());
        }
    }
    if parsed.len() != 2 {
        return Err("V32 CPU preflight arguments differ".to_owned());
    }
    let mode = match parsed.get("--mode").map(String::as_str) {
        Some("probe") => V32CpuPreflightMode::Probe,
        Some("screen") => V32CpuPreflightMode::Screen,
        _ => return Err("V32 CPU preflight mode differs".to_owned()),
    };
    let leaf_beam = parsed
        .get("--leaf-beam")
        .ok_or_else(|| "V32 CPU preflight leaf beam is missing".to_owned())?
        .parse::<usize>()
        .map_err(|_| "V32 CPU preflight leaf beam differs".to_owned())?;
    if !matches!(leaf_beam, 64 | 128 | 256) {
        return Err("V32 CPU preflight leaf beam differs".to_owned());
    }
    Ok((mode, leaf_beam))
}

#[cfg(not(test))]
fn main() {
    let result = parse_args(std::env::args().skip(1))
        .and_then(|(mode, leaf_beam)| {
            run_v32_cpu_preflight(mode, leaf_beam).map_err(|e| e.to_string())
        })
        .and_then(|bytes| {
            std::io::stdout()
                .write_all(&bytes)
                .map_err(|error| error.to_string())
        });
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::parse_args;
    use borsuk::V32CpuPreflightMode;

    #[test]
    fn v32_cpu_preflight_cli_accepts_only_registered_modes_and_arms() {
        let parsed = parse_args(["--mode", "probe", "--leaf-beam", "64"]).unwrap();
        assert_eq!(parsed, (V32CpuPreflightMode::Probe, 64));
        let parsed = parse_args(["--mode", "screen", "--leaf-beam", "256"]).unwrap();
        assert_eq!(parsed, (V32CpuPreflightMode::Screen, 256));
        for invalid in [
            vec!["--mode", "probe", "--leaf-beam", "32"],
            vec!["--mode", "other", "--leaf-beam", "64"],
            vec!["--mode", "probe"],
            vec!["--mode", "probe", "--mode", "screen", "--leaf-beam", "64"],
            vec!["--mode", "probe", "--leaf-beam", "64", "--s3", "bucket"],
        ] {
            assert!(parse_args(invalid).is_err());
        }
    }
}
