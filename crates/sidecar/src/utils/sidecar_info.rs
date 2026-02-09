use crate::args;

pub fn print_sidecar_info(config: &args::Config) {
    use tracing::info;

    let version = env!("CARGO_PKG_VERSION");
    let git_commit = option_env!("SIDECAR_GIT_SHA").unwrap_or("unknown");
    let rustc_version = option_env!("SIDECAR_RUSTC_VERSION").unwrap_or("unknown");
    let cpu_cores = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(0);
    let memory_available = available_memory_info();
    let os_info = run_command("uname", &["-a"])
        .unwrap_or_else(|| format!("{} {}", std::env::consts::OS, std::env::consts::ARCH));

    info!(version, git_commit, rustc_version, "Sidecar build info");
    info!(
        cpu_cores,
        memory_available = %memory_available,
        os_info = %os_info,
        "Sidecar host info"
    );
    info!("Sidecar config: {config:?}");
}

fn run_command(command: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(command)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn available_memory_info() -> String {
    #[cfg(target_os = "linux")]
    {
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if let Some(value) = line.strip_prefix("MemAvailable:") {
                    return value.trim().to_string();
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(vm_stat) = run_command("vm_stat", &[])
            && let Some(parsed) = parse_vm_stat_available(&vm_stat)
        {
            return parsed;
        }
    }

    "unknown".to_string()
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_available(vm_stat: &str) -> Option<String> {
    let page_size = parse_vm_stat_page_size(vm_stat)?;
    let free = parse_vm_stat_pages(vm_stat, "Pages free:")?;
    let inactive = parse_vm_stat_pages(vm_stat, "Pages inactive:").unwrap_or(0);
    let speculative = parse_vm_stat_pages(vm_stat, "Pages speculative:").unwrap_or(0);

    let available_pages = free.saturating_add(inactive).saturating_add(speculative);
    let available_bytes = available_pages.saturating_mul(page_size);
    let available_mib = available_bytes / (1024 * 1024);
    Some(format!("{available_mib} MiB"))
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_page_size(vm_stat: &str) -> Option<u64> {
    let first_line = vm_stat.lines().next()?;
    let marker = "page size of ";
    let start = first_line.find(marker)? + marker.len();
    let rest = &first_line[start..];
    let end = rest.find(" bytes")?;
    rest[..end].trim().parse::<u64>().ok()
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_pages(vm_stat: &str, prefix: &str) -> Option<u64> {
    let line = vm_stat
        .lines()
        .find(|line| line.trim_start().starts_with(prefix))?;
    let value = line
        .split(':')
        .nth(1)?
        .trim()
        .trim_end_matches('.')
        .replace('.', "");
    value.parse::<u64>().ok()
}
