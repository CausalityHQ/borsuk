//! Linux child lifecycle CPU accounting; root-owned cgroup qualification is required.
//!
//! CLI: `native_process_cpu CONFIG CONFIG_SHA256 NEW_REPORT` or `--fixture CASE`.
//! Config: deny-unknown-fields JSON `{"schema":"native-process-cpu-config-v1",
//! "executable":{"path":"/absolute/ELF","bytes":123,"sha256":"lowercase hex"},
//! "args":["literal argument"]}`. SHA authenticates the exact config bytes.
//! Bounds: config 64 KiB, 64 args / 4096 bytes each / 16 KiB including terminators,
//! paths 4096 bytes, executable 512 MiB streamed in 64 KiB, report 16 KiB.
//! Root must enforce deadline/resources, immutable source/runtime dependencies,
//! trusted stable directories, sanitized environment and per-invocation cgroup
//! placement/drain. This helper cannot establish descendant completeness or total
//! host CPU. CPU nanoseconds are exact unit conversions, not accuracy claims.
//! wait4 excludes descendants not waited within the child's hierarchy, whether
//! still live, terminated without a wait, or reaped elsewhere.
//! Preserve stderr: root must require the matching collector REPORT_SYNCED receipt
//! as well as the report. Child exit alone cannot certify accounting/publication.
//! No internal timeout, cgroup operations, retry of an invocation or cleanup kill.
//! Before any spawn, set and verify this helper's inherited RLIMIT_CORE=0;
//! failure exits125 before any child. No host/global core setting is changed.
//!
//! Remote method falsifier ONLY: `--fixture pulse|threads|waited|io|late|delay|
//! nonzero|survivor|unwaited|sigpipe`. Each needs a fresh root-owned disposable cgroup;
//! `survivor` deliberately leaves a 2-second descendant and MUST be refused by
//! root even if it later drains. `unwaited` observes its >=20ms-CPU `late` child
//! terminate with waitid(WNOWAIT), then exits without reaping it. Both require
//! BORSUK_NATIVE_CPU_DISPOSABLE_CGROUP=1 as root's context attestation, not proof.
//! `sigpipe` emits and flushes a 2ms CPU witness, then restores/unblocks/raises SIGPIPE.
//! Ten named cases consume13 processes, real-child test1, publication-failure
//! pulse1 and timeout-KILL delay1 total16; CPU1/256 MiB/swap0/60s/ext4/no network.
//! Root owns cgroup discrepancy/refusal and closed-artifact receipt/counter
//! mutations. This component does not qualify the separate query-wall gate.

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("native_process_cpu requires Linux; no child was started");
    std::process::exit(125);
}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        env,
        fs::{File, OpenOptions},
        io::{self, Read, Seek, SeekFrom, Write},
        mem::MaybeUninit,
        os::{
            fd::AsRawFd,
            unix::{
                ffi::OsStrExt,
                fs::{OpenOptionsExt, PermissionsExt},
                process::{CommandExt, ExitStatusExt},
            },
        },
        path::Path,
        process::{Command, ExitStatus, Stdio},
        time::Duration,
    };

    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};

    const CONFIG_SCHEMA: &str = "native-process-cpu-config-v1";
    const MAX_CONFIG_BYTES: usize = 65_536;
    const MAX_REPORT_BYTES: usize = 16_384;
    const MAX_PATH_BYTES: usize = 4_096;
    const MAX_ARGS: usize = 64;
    const MAX_ARG_BYTES: usize = 4_096;
    const MAX_ARGV_BYTES: usize = 16_384;
    const MAX_EXECUTABLE_BYTES: u64 = 536_870_912;

    fn invalid(message: &str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, message)
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct Artifact {
        path: String,
        bytes: u64,
        sha256: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        schema: String,
        executable: Artifact,
        args: Vec<String>,
    }

    fn valid_sha(sha: &str) -> bool {
        sha.len() == 64
            && sha
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    fn validate_path(path: &Path) -> io::Result<()> {
        let bytes = path.as_os_str().as_bytes();
        if !path.is_absolute() || bytes.len() > MAX_PATH_BYTES || bytes.contains(&0) {
            return Err(invalid("path must be absolute, NUL-free and <=4096 bytes"));
        }
        Ok(())
    }

    impl Config {
        fn validate(&self) -> io::Result<()> {
            validate_path(Path::new(&self.executable.path))?;
            if self.schema != CONFIG_SCHEMA
                || self.executable.bytes == 0
                || self.executable.bytes > MAX_EXECUTABLE_BYTES
                || !valid_sha(&self.executable.sha256)
                || self.args.len() > MAX_ARGS
            {
                return Err(invalid("invalid schema, executable pin or argument count"));
            }
            let mut bytes = 0usize;
            for arg in &self.args {
                if arg.len() > MAX_ARG_BYTES || arg.as_bytes().contains(&0) {
                    return Err(invalid("argument exceeds bound or contains NUL"));
                }
                bytes = bytes
                    .checked_add(arg.len() + 1)
                    .ok_or_else(|| invalid("argv overflow"))?;
            }
            if bytes > MAX_ARGV_BYTES {
                return Err(invalid("argv exceeds 16 KiB"));
            }
            Ok(())
        }
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn decode_config(bytes: &[u8], sha: &str) -> io::Result<Config> {
        if bytes.len() > MAX_CONFIG_BYTES || !valid_sha(sha) || digest(bytes) != sha {
            return Err(invalid("config size or SHA256 mismatch"));
        }
        let config: Config = serde_json::from_slice(bytes).map_err(io::Error::other)?;
        config.validate()?;
        Ok(config)
    }

    fn open_regular(path: &Path) -> io::Result<File> {
        validate_path(path)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        if !file.metadata()?.is_file() {
            return Err(invalid("input is not a regular file"));
        }
        Ok(file)
    }

    fn read_config(path: &Path, sha: &str) -> io::Result<Config> {
        let file = open_regular(path)?;
        if file.metadata()?.len() > MAX_CONFIG_BYTES as u64 {
            return Err(invalid("config exceeds 64 KiB"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        decode_config(&bytes, sha)
    }

    fn hash_file(file: &mut File, expected_bytes: u64) -> io::Result<String> {
        if expected_bytes == 0
            || expected_bytes > MAX_EXECUTABLE_BYTES
            || file.metadata()?.len() != expected_bytes
        {
            return Err(invalid("executable size mismatch or bound exceeded"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut input = file.take(expected_bytes + 1);
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 65_536];
        let mut total = 0u64;
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            total = total
                .checked_add(count as u64)
                .ok_or_else(|| invalid("read count overflow"))?;
            hasher.update(&buffer[..count]);
        }
        if total != expected_bytes {
            return Err(invalid("executable changed size during authentication"));
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    fn pin_executable(artifact: &Artifact) -> io::Result<File> {
        let mut file = open_regular(Path::new(&artifact.path))?;
        if !valid_sha(&artifact.sha256) || file.metadata()?.permissions().mode() & 0o111 == 0 {
            return Err(invalid("invalid executable hash or mode"));
        }
        let mut magic = [0; 4];
        file.read_exact(&mut magic)?;
        if magic != *b"\x7fELF" || hash_file(&mut file, artifact.bytes)? != artifact.sha256 {
            return Err(invalid("executable must be an authenticated ELF file"));
        }
        Ok(file)
    }

    struct ReportFile {
        file: File,
        parent: File,
    }

    impl ReportFile {
        fn reserve(path: &Path) -> io::Result<Self> {
            validate_path(path)?;
            let parent_path = path
                .parent()
                .ok_or_else(|| invalid("report has no parent"))?;
            let parent = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(parent_path)?;
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            // Retain empty/partial evidence on every failure; never unlink or overwrite it.
            file.sync_all()?;
            parent.sync_all()?;
            Ok(Self { file, parent })
        }

        fn publish(&mut self, bytes: &[u8]) -> io::Result<()> {
            if bytes.len() > MAX_REPORT_BYTES {
                return Err(invalid("report exceeds 16 KiB"));
            }
            self.file.write_all(bytes)?;
            self.file.sync_all()?;
            self.parent.sync_all()
        }
    }

    #[derive(Debug, Serialize)]
    struct Timeval {
        seconds: i64,
        microseconds: i64,
        cpu_ns: u64,
    }

    impl Timeval {
        fn checked(seconds: i64, microseconds: i64) -> io::Result<Self> {
            if seconds < 0 || !(0..1_000_000).contains(&microseconds) {
                return Err(invalid("negative or unnormalized CPU timeval"));
            }
            let cpu_ns = (seconds as u64)
                .checked_mul(1_000_000_000)
                .and_then(|ns| ns.checked_add(microseconds as u64 * 1_000))
                .ok_or_else(|| invalid("CPU nanoseconds overflow"))?;
            Ok(Self {
                seconds,
                microseconds,
                cpu_ns,
            })
        }
    }

    #[derive(Debug, Serialize)]
    struct CpuUsage {
        user: Timeval,
        system: Timeval,
        cpu_ns: u64,
    }

    impl CpuUsage {
        fn from_fields(
            user_sec: i64,
            user_usec: i64,
            system_sec: i64,
            system_usec: i64,
        ) -> io::Result<Self> {
            let user = Timeval::checked(user_sec, user_usec)?;
            let system = Timeval::checked(system_sec, system_usec)?;
            let cpu_ns = user
                .cpu_ns
                .checked_add(system.cpu_ns)
                .ok_or_else(|| invalid("user+system overflow"))?;
            Ok(Self {
                user,
                system,
                cpu_ns,
            })
        }

        fn from_raw(raw: libc::rusage) -> io::Result<Self> {
            Self::from_fields(
                raw.ru_utime.tv_sec as i64,
                raw.ru_utime.tv_usec as i64,
                raw.ru_stime.tv_sec as i64,
                raw.ru_stime.tv_usec as i64,
            )
        }
    }

    fn self_usage() -> io::Result<CpuUsage> {
        let mut raw = MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the valid output pointer on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, raw.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        CpuUsage::from_raw(unsafe { raw.assume_init() })
    }

    fn clock_ns(clock: libc::clockid_t) -> io::Result<u64> {
        let mut raw = MaybeUninit::<libc::timespec>::uninit();
        // SAFETY: clock_gettime initializes the valid output pointer on success.
        if unsafe { libc::clock_gettime(clock, raw.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let raw = unsafe { raw.assume_init() };
        if raw.tv_sec < 0 || !(0..1_000_000_000).contains(&raw.tv_nsec) {
            return Err(invalid("invalid native clock fields"));
        }
        (raw.tv_sec as u64)
            .checked_mul(1_000_000_000)
            .and_then(|ns| ns.checked_add(raw.tv_nsec as u64))
            .ok_or_else(|| invalid("native clock overflow"))
    }

    fn checked_delta(end: u64, start: u64) -> io::Result<u64> {
        end.checked_sub(start)
            .ok_or_else(|| invalid("clock/accounting moved backwards"))
    }

    #[derive(Debug, Serialize)]
    struct ChildStatus {
        raw_wait_status: i32,
        exit_code: Option<i32>,
        signal: Option<i32>,
        core_dumped: bool,
    }

    impl ChildStatus {
        fn checked(raw: i32) -> io::Result<Self> {
            let status = ExitStatus::from_raw(raw);
            if status.code().is_none() && status.signal().is_none() {
                return Err(invalid("wait4 did not return a terminal status"));
            }
            Ok(Self {
                raw_wait_status: raw,
                exit_code: status.code(),
                signal: status.signal(),
                core_dumped: status.core_dumped(),
            })
        }
    }

    fn wait_child(pid: libc::pid_t) -> io::Result<(i32, libc::rusage)> {
        loop {
            let mut status = 0;
            let mut usage = MaybeUninit::<libc::rusage>::uninit();
            // SAFETY: exact owned child PID; wait4 writes both outputs on success.
            let got = unsafe { libc::wait4(pid, &mut status, 0, usage.as_mut_ptr()) };
            if got == pid {
                return Ok((status, unsafe { usage.assume_init() }));
            }
            let error = io::Error::last_os_error();
            if got == -1 && error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
    }

    #[derive(Serialize)]
    struct Report<'a> {
        schema: &'static str,
        status: &'static str,
        config_sha256: &'a str,
        executable_sha256: &'a str,
        executable_bytes: u64,
        argument_count: usize,
        child_pid: u32,
        child_status: ChildStatus,
        child_cpu: CpuUsage,
        before_spawn_monotonic_ns: u64,
        after_reap_monotonic_ns: u64,
        lifecycle_wall_bound_ns: u64,
        helper_self_at_entry: CpuUsage,
        helper_self_before_report: CpuUsage,
        helper_observed_cpu_ns: u64,
        accounting_scope: &'static str,
        completeness: &'static str,
        helper_scope: &'static str,
    }

    fn run(
        config_path: &Path,
        sha: &str,
        report_path: &Path,
        self_at_entry: CpuUsage,
    ) -> io::Result<ExitStatus> {
        let config = read_config(config_path, sha)?;
        let executable = pin_executable(&config.executable)?;
        let mut output = ReportFile::reserve(report_path)?;
        let before = clock_ns(libc::CLOCK_MONOTONIC)?;
        // /proc/self/fd pins the opened inode, including across a path replacement.
        let mut child = Command::new(format!("/proc/self/fd/{}", executable.as_raw_fd()))
            .arg0(&config.executable.path)
            .args(&config.args)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        let pid = child.id();
        // Linux child IDs originate in a positive pid_t and therefore fit this cast.
        let (raw_status, raw_usage) = match wait_child(pid as libc::pid_t) {
            Ok(reaped) => reaped,
            Err(error) => {
                let status = child.wait()?;
                let _ = collector_receipt(&mut io::stderr().lock(), sha, status, Err(error));
                return Ok(status);
            }
        };
        let status = ExitStatus::from_raw(raw_status);
        let publication = (|| -> io::Result<()> {
            let after = clock_ns(libc::CLOCK_MONOTONIC)?;
            let child_cpu = CpuUsage::from_raw(raw_usage)?;
            let helper = self_usage()?;
            let helper_observed_cpu_ns = checked_delta(helper.cpu_ns, self_at_entry.cpu_ns)?;
            let report = Report {
                schema: "native-process-cpu-report-v1",
                status: "REAPED_ACCOUNTING_ONLY",
                config_sha256: sha,
                executable_sha256: &config.executable.sha256,
                executable_bytes: config.executable.bytes,
                argument_count: config.args.len(),
                child_pid: pid,
                child_status: ChildStatus::checked(raw_status)?,
                child_cpu,
                before_spawn_monotonic_ns: before,
                after_reap_monotonic_ns: after,
                lifecycle_wall_bound_ns: checked_delta(after, before)?,
                helper_self_at_entry: self_at_entry,
                helper_self_before_report: helper,
                helper_observed_cpu_ns,
                accounting_scope: "Linux wait4 direct child: all its threads and descendants whose usage was propagated by waits; creation through reaping; excludes ALL descendants not waited within the child's hierarchy, whether live, terminated without a wait or reaped elsewhere, and host CPU charged elsewhere",
                completeness: "UNVERIFIED: root must establish per-invocation cgroup placement, refuse survivors and verify drain/accounting; reaping alone is insufficient",
                helper_scope: "RUSAGE_SELF snapshots; delta includes config/executable authentication, output reservation and wait; excludes startup before entry and report encoding/write/sync, collector receipt, destruction/exit after snapshot; never subtracted; root must bound all omitted and treatment-dependent supervisor cost",
            };
            let mut bytes = serde_json::to_vec(&report).map_err(io::Error::other)?;
            bytes.push(b'\n');
            output.publish(&bytes)
        })();
        let _ = collector_receipt(&mut io::stderr().lock(), sha, status, publication);
        Ok(status)
    }

    fn collector_receipt(
        writer: &mut impl Write,
        sha: &str,
        child: ExitStatus,
        publication: io::Result<()>,
    ) -> io::Result<()> {
        let error = publication.err().map(|error| error.to_string());
        let collector_status = if error.is_none() {
            "REPORT_SYNCED"
        } else {
            "ACCOUNTING_OR_PUBLICATION_FAILED"
        };
        // Inherited child stderr need not end in LF.
        writeln!(
            writer,
            "\n{}",
            serde_json::json!({
                "schema": "native-process-cpu-collector-v1", "config_sha256": sha,
                "collector_status": collector_status, "raw_child_wait_status": child.into_raw(),
                "child_exit_code": child.code(), "child_signal": child.signal(),
                "child_core_dumped": child.core_dumped(), "error": error
            })
        )
    }

    fn finish(status: ExitStatus) -> ! {
        if let Some(code) = status.code() {
            std::process::exit(code);
        }
        if let Some(signal) = status.signal() {
            // SAFETY: restore default disposition and unblock only the child's signal.
            unsafe {
                libc::signal(signal, libc::SIG_DFL);
                let mut set = MaybeUninit::<libc::sigset_t>::uninit();
                libc::sigemptyset(set.as_mut_ptr());
                let mut set = set.assume_init();
                libc::sigaddset(&mut set, signal);
                libc::sigprocmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
                libc::raise(signal);
            }
            std::process::exit(128 + signal);
        }
        std::process::exit(125);
    }

    fn disable_core_dumps() -> io::Result<()> {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: valid input/output pointers; limits affect only this process and its future children.
        if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut actual = MaybeUninit::<libc::rlimit>::uninit();
        if unsafe { libc::getrlimit(libc::RLIMIT_CORE, actual.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let actual = unsafe { actual.assume_init() };
        if actual.rlim_cur != 0 || actual.rlim_max != 0 {
            return Err(invalid("RLIMIT_CORE=0 could not be established"));
        }
        Ok(())
    }

    pub(super) fn main() {
        let result = (|| -> io::Result<ExitStatus> {
            let entry = self_usage()?;
            disable_core_dumps()?;
            let args: Vec<_> = env::args_os().skip(1).take(4).collect();
            if args.len() == 2 && args[0] == "--fixture" {
                let case = args[1]
                    .to_str()
                    .ok_or_else(|| invalid("fixture must be UTF-8"))?;
                return fixture(case);
            }
            if args.len() != 3 {
                return Err(invalid(
                    "usage: native_process_cpu CONFIG CONFIG_SHA256 NEW_REPORT | --fixture CASE",
                ));
            }
            let sha = args[1]
                .to_str()
                .ok_or_else(|| invalid("SHA256 must be UTF-8"))?;
            run(Path::new(&args[0]), sha, Path::new(&args[2]), entry)
        })();
        match result {
            Ok(status) => finish(status),
            Err(error) => {
                eprintln!("native_process_cpu: {error}");
                std::process::exit(125);
            }
        }
    }

    fn burn_cpu(target_ns: u64) -> io::Result<u64> {
        let start = clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)?;
        loop {
            for value in 0..1_024u64 {
                std::hint::black_box(value.wrapping_mul(value));
            }
            let elapsed = checked_delta(clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)?, start)?;
            if elapsed >= target_ns {
                return Ok(elapsed);
            }
        }
    }

    fn fixture(case: &str) -> io::Result<ExitStatus> {
        if matches!(case, "survivor" | "unwaited")
            && !matches!(
                env::var("BORSUK_NATIVE_CPU_DISPOSABLE_CGROUP").as_deref(),
                Ok("1")
            )
        {
            return Err(invalid(
                "survivor/unwaited require root-owned disposable cgroup and BORSUK_NATIVE_CPU_DISPOSABLE_CGROUP=1",
            ));
        }
        let start = clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)?;
        let mut injected_cpu_ns = 0;
        let mut waited_child_cpu_ns = None;
        let mut descendant_pid = None;
        let mut unwaited_event = None;
        let mut exit_code = 0;
        match case {
            "pulse" => injected_cpu_ns = burn_cpu(2_000_000)?,
            "threads" => {
                let a = std::thread::spawn(|| burn_cpu(5_000_000));
                let b = std::thread::spawn(|| burn_cpu(5_000_000));
                a.join()
                    .map_err(|_| io::Error::other("fixture thread panicked"))??;
                b.join()
                    .map_err(|_| io::Error::other("fixture thread panicked"))??;
                injected_cpu_ns = checked_delta(clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)?, start)?;
            }
            "waited" => {
                #[allow(clippy::zombie_processes)] // Reaped by exact-PID wait4 below.
                let child = Command::new(env::current_exe()?)
                    .args(["--fixture", "pulse"])
                    .stdin(Stdio::null())
                    .spawn()?;
                let (raw, usage) = wait_child(child.id() as libc::pid_t)?;
                if !ExitStatus::from_raw(raw).success() {
                    return Err(io::Error::other("waited fixture failed"));
                }
                waited_child_cpu_ns = Some(CpuUsage::from_raw(usage)?.cpu_ns);
            }
            "io" => {
                let mut buffer = [0; 65_536];
                let mut scratch = tempfile::tempfile()?;
                for _ in 0..16 {
                    let count = File::open("/proc/self/status")?
                        .take(buffer.len() as u64)
                        .read(&mut buffer)?;
                    scratch.write_all(&buffer[..count])?;
                }
                scratch.sync_all()?;
            }
            "late" => {
                let query_cpu_ns = burn_cpu(2_000_000)?;
                let fake_terminal_cpu_ns = clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)?;
                println!(
                    "{{\"fixture\":\"late\",\"query_cpu_ns\":{query_cpu_ns},\"fake_terminal_cpu_ns\":{fake_terminal_cpu_ns}}}"
                );
                io::stdout().flush()?;
                injected_cpu_ns = burn_cpu(20_000_000)?;
                let mut scratch = tempfile::tempfile()?;
                serde_json::to_writer(&mut scratch, &vec![injected_cpu_ns; 1_024])
                    .map_err(io::Error::other)?;
                scratch.sync_all()?;
            }
            "delay" => std::thread::sleep(Duration::from_secs(2)),
            "nonzero" => {
                io::stderr().write_all(b"nonzero fixture stderr without LF")?;
                exit_code = 7;
            }
            "unwaited" => {
                #[allow(clippy::zombie_processes)]
                // Deliberately observe termination without reaping.
                let child = Command::new(env::current_exe()?)
                    .args(["--fixture", "late"])
                    .stdin(Stdio::null())
                    .spawn()?;
                let pid = child.id() as libc::pid_t;
                let info = loop {
                    let mut info = MaybeUninit::<libc::siginfo_t>::zeroed();
                    // SAFETY: exact owned child PID and valid output; WNOWAIT retains its waitable status.
                    if unsafe {
                        libc::waitid(
                            libc::P_PID,
                            pid as libc::id_t,
                            info.as_mut_ptr(),
                            libc::WEXITED | libc::WNOWAIT,
                        )
                    } == 0
                    {
                        break unsafe { info.assume_init() };
                    }
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                };
                // SAFETY: successful WEXITED waitid initialized the SIGCHLD event union.
                let observed_pid = unsafe { info.si_pid() };
                let status = unsafe { info.si_status() };
                if info.si_signo != libc::SIGCHLD
                    || observed_pid != pid
                    || info.si_code != libc::CLD_EXITED
                    || status != 0
                {
                    return Err(invalid(
                        "unwaited late child did not exit0 with the expected PID",
                    ));
                }
                unwaited_event = Some(
                    serde_json::json!({"pid":observed_pid, "si_signo":info.si_signo,
                    "si_code":info.si_code, "si_status":status, "observed_with":"WEXITED|WNOWAIT",
                    "scope":"terminated child deliberately never reaped here; its CPU is missing from the parent wait4 hierarchy"}),
                );
                drop(child);
            }
            "sigpipe" => {
                let injected_cpu_ns = burn_cpu(2_000_000)?;
                println!(
                    "{}",
                    serde_json::json!({"fixture":"sigpipe", "event":"before_signal",
                    "injected_cpu_ns":injected_cpu_ns, "expected_signal":libc::SIGPIPE})
                );
                io::stdout().flush()?;
                finish(ExitStatus::from_raw(libc::SIGPIPE));
            }
            "survivor" => {
                // Deliberate falsifier: root must observe/refuse this live descendant.
                #[allow(clippy::zombie_processes)]
                let child = Command::new(env::current_exe()?)
                    .args(["--fixture", "delay"])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?;
                descendant_pid = Some(child.id());
                drop(child);
            }
            _ => {
                return Err(invalid(
                    "unknown fixture; expected pulse,threads,waited,io,late,delay,nonzero,survivor,unwaited,sigpipe",
                ));
            }
        }
        let process_cpu_ns = checked_delta(clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)?, start)?;
        println!(
            "{}",
            serde_json::json!({"fixture":case, "injected_cpu_ns":injected_cpu_ns,
            "process_cpu_ns":process_cpu_ns, "waited_child_cpu_ns":waited_child_cpu_ns,
            "surviving_descendant_pid":descendant_pid, "unwaited_event":unwaited_event,
            "scope":"fixture body only; excludes final stdout/destruction/exit"})
        );
        Ok(ExitStatus::from_raw(exit_code << 8))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::fs;
        use std::os::unix::fs::{PermissionsExt, symlink};

        fn config(path: &Path) -> Config {
            Config {
                schema: CONFIG_SCHEMA.into(),
                executable: Artifact {
                    path: path.to_str().unwrap().into(),
                    bytes: 4,
                    sha256: digest(b"\x7fELF"),
                },
                args: vec![],
            }
        }

        #[test]
        fn integer_accounting_rejects_invalid_fields_and_overflow() {
            let usage = CpuUsage::from_fields(2, 3, 4, 5).unwrap();
            assert_eq!(usage.user.cpu_ns, 2_000_003_000);
            assert_eq!(usage.system.cpu_ns, 4_000_005_000);
            assert_eq!(usage.cpu_ns, 6_000_008_000);
            assert!(CpuUsage::from_fields(-1, 0, 0, 0).is_err());
            assert!(CpuUsage::from_fields(0, -1, 0, 0).is_err());
            assert!(CpuUsage::from_fields(0, 1_000_000, 0, 0).is_err());
            assert!(CpuUsage::from_fields(i64::MAX, 0, 0, 0).is_err());
            assert!(CpuUsage::from_fields(10_000_000_000, 0, 10_000_000_000, 0).is_err());
            assert_eq!(Timeval::checked(0, 1).unwrap().cpu_ns, 1_000);
            assert!(checked_delta(1, 2).is_err());
        }

        #[test]
        fn wait_status_keeps_exit_signal_and_core_flag() {
            assert_eq!(ChildStatus::checked(7 << 8).unwrap().exit_code, Some(7));
            assert_eq!(ChildStatus::checked(0).unwrap().exit_code, Some(0));
            let signal = ChildStatus::checked(libc::SIGTERM).unwrap();
            assert_eq!(signal.signal, Some(libc::SIGTERM));
            assert_eq!(signal.exit_code, None);
            assert_eq!(
                ChildStatus::checked(libc::SIGPIPE).unwrap().signal,
                Some(libc::SIGPIPE)
            );
            assert_eq!(ChildStatus::checked(libc::SIGPIPE).unwrap().exit_code, None);
            assert!(
                ChildStatus::checked(libc::SIGSEGV | 0x80)
                    .unwrap()
                    .core_dumped
            );
            assert!(ChildStatus::checked((libc::SIGSTOP << 8) | 0x7f).is_err());
        }

        #[test]
        fn collector_failure_is_separate_from_original_child_status() {
            for raw in [0, 7 << 8, libc::SIGTERM, libc::SIGPIPE] {
                for publication_ok in [true, false] {
                    let mut bytes = b"unterminated child stderr".to_vec();
                    let sha = "a".repeat(64);
                    let child = ExitStatus::from_raw(raw);
                    let publication = if publication_ok {
                        Ok(())
                    } else {
                        Err(io::Error::other("sync failed"))
                    };
                    collector_receipt(&mut bytes, &sha, child, publication).unwrap();
                    assert!(bytes.ends_with(b"\n"));
                    let mut lines = std::str::from_utf8(&bytes).unwrap().lines();
                    assert_eq!(lines.next(), Some("unterminated child stderr"));
                    let receipt: serde_json::Value =
                        serde_json::from_str(lines.next().unwrap()).unwrap();
                    assert!(lines.next().is_none());
                    assert_eq!(receipt["schema"], "native-process-cpu-collector-v1");
                    assert_eq!(receipt["config_sha256"], sha);
                    assert_eq!(
                        receipt["collector_status"],
                        if publication_ok {
                            "REPORT_SYNCED"
                        } else {
                            "ACCOUNTING_OR_PUBLICATION_FAILED"
                        }
                    );
                    assert_eq!(receipt["raw_child_wait_status"], raw);
                    assert_eq!(receipt["child_exit_code"], serde_json::json!(child.code()));
                    assert_eq!(receipt["child_signal"], serde_json::json!(child.signal()));
                    assert_eq!(
                        receipt["error"],
                        if publication_ok {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!("sync failed")
                        }
                    );
                    assert!(bytes.len() <= MAX_REPORT_BYTES);
                }
            }
        }

        #[test]
        fn config_authentication_schema_and_argument_bounds() {
            let mut cfg = config(Path::new("/tmp/fixture"));
            let bytes = serde_json::to_vec(&cfg).unwrap();
            assert!(decode_config(&bytes, &digest(&bytes)).is_ok());
            assert!(decode_config(&bytes, &"0".repeat(64)).is_err());
            assert!(decode_config(&bytes, &"A".repeat(64)).is_err());
            let unknown = [b"{\"unknown\":0,".as_slice(), &bytes[1..]].concat();
            assert!(decode_config(&unknown, &digest(&unknown)).is_err());
            let duplicate = [
                b"{\"schema\":\"native-process-cpu-config-v1\",".as_slice(),
                &bytes[1..],
            ]
            .concat();
            assert!(decode_config(&duplicate, &digest(&duplicate)).is_err());
            cfg.args = vec!["x".repeat(MAX_ARG_BYTES + 1)];
            assert!(cfg.validate().is_err());
            cfg.args = vec!["".into(); MAX_ARGS + 1];
            assert!(cfg.validate().is_err());
            cfg.args = vec!["x".repeat(MAX_ARG_BYTES); 4];
            assert!(cfg.validate().is_err());
            cfg.args = vec!["a\0b".into()];
            assert!(cfg.validate().is_err());
            cfg.args.clear();
            cfg.executable.path = "relative".into();
            assert!(cfg.validate().is_err());
            cfg.executable.path = "/tmp/fixture".into();
            cfg.executable.bytes = MAX_EXECUTABLE_BYTES + 1;
            assert!(cfg.validate().is_err());
            assert!(decode_config(&vec![b' '; MAX_CONFIG_BYTES + 1], &"0".repeat(64)).is_err());
        }

        #[test]
        fn source_and_config_refuse_symlink_size_hash_and_non_elf() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("executable");
            fs::write(&path, b"\x7fELF").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            let mut cfg = config(&path);
            assert!(pin_executable(&cfg.executable).is_ok());
            cfg.executable.sha256 = "0".repeat(64);
            assert!(pin_executable(&cfg.executable).is_err());
            cfg.executable.sha256 = digest(b"\x7fELF");
            cfg.executable.bytes = 5;
            assert!(pin_executable(&cfg.executable).is_err());
            cfg.executable.bytes = 4;
            fs::write(&path, b"nope").unwrap();
            cfg.executable.sha256 = digest(b"nope");
            assert!(pin_executable(&cfg.executable).is_err());
            let link = dir.path().join("link");
            symlink(&path, &link).unwrap();
            cfg.executable.path = link.to_str().unwrap().into();
            assert!(pin_executable(&cfg.executable).is_err());
            assert!(read_config(&link, &"0".repeat(64)).is_err());
            fs::write(&path, vec![b' '; MAX_CONFIG_BYTES + 1]).unwrap();
            assert!(read_config(&path, &"0".repeat(64)).is_err());
            assert!(read_config(dir.path(), &"0".repeat(64)).is_err());

            let config_path = dir.path().join("config.json");
            let report_path = dir.path().join("must-not-exist.json");
            let bytes = serde_json::to_vec(&cfg).unwrap();
            fs::write(&config_path, &bytes).unwrap();
            assert!(
                run(
                    &config_path,
                    &digest(&bytes),
                    &report_path,
                    self_usage().unwrap()
                )
                .is_err()
            );
            assert!(!report_path.exists());
        }

        #[test]
        fn output_is_create_only_and_partial_evidence_is_retained() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("report.json");
            fs::write(&path, b"existing evidence").unwrap();
            assert!(ReportFile::reserve(&path).is_err());
            assert_eq!(fs::read(&path).unwrap(), b"existing evidence");
            let link = dir.path().join("link");
            symlink(&path, &link).unwrap();
            assert!(ReportFile::reserve(&link).is_err());
            let fresh = dir.path().join("new.json");
            let mut report = ReportFile::reserve(&fresh).unwrap();
            assert_eq!(
                fs::metadata(&fresh).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert!(report.publish(&vec![0; MAX_REPORT_BYTES + 1]).is_err());
            assert_eq!(fs::metadata(&fresh).unwrap().len(), 0);
            report.publish(b"{}\n").unwrap();
            assert_eq!(fs::read(&fresh).unwrap(), b"{}\n");
            assert!(ReportFile::reserve(Path::new("relative")).is_err());
        }

        // Invoked only by real_child_accounting, in a separately reaped process.
        #[test]
        #[ignore = "child process fixture; exits with code 7"]
        fn child_cpu_exit() {
            disable_core_dumps().unwrap();
            burn_cpu(2_000_000).unwrap();
            std::process::exit(7);
        }

        #[test]
        fn real_child_accounting() {
            let dir = tempfile::tempdir().unwrap();
            let executable = env::current_exe().unwrap();
            let mut cfg = config(&executable);
            cfg.executable.bytes = fs::metadata(&executable).unwrap().len();
            cfg.executable.sha256 = hash_file(
                &mut open_regular(&executable).unwrap(),
                cfg.executable.bytes,
            )
            .unwrap();
            cfg.args = vec![
                "--exact".into(),
                "linux::tests::child_cpu_exit".into(),
                "--ignored".into(),
                "--nocapture".into(),
            ];
            let config_bytes = serde_json::to_vec(&cfg).unwrap();
            let config_path = dir.path().join("config.json");
            fs::write(&config_path, &config_bytes).unwrap();
            let report_path = dir.path().join("report.json");
            let status = run(
                &config_path,
                &digest(&config_bytes),
                &report_path,
                self_usage().unwrap(),
            )
            .unwrap();
            assert_eq!(status.code(), Some(7));
            let report: serde_json::Value =
                serde_json::from_slice(&fs::read(&report_path).unwrap()).unwrap();
            assert_eq!(report["status"], "REAPED_ACCOUNTING_ONLY");
            assert_eq!(report["child_status"]["exit_code"], 7);
            assert!(report["child_cpu"]["cpu_ns"].as_u64().unwrap() >= 2_000_000);
            assert!(
                report["after_reap_monotonic_ns"].as_u64().unwrap()
                    >= report["before_spawn_monotonic_ns"].as_u64().unwrap()
            );
            let original = fs::read(&report_path).unwrap();
            assert!(
                run(
                    &config_path,
                    &digest(&config_bytes),
                    &report_path,
                    self_usage().unwrap()
                )
                .is_err()
            );
            assert_eq!(fs::read(&report_path).unwrap(), original);
        }
    }
}
