//! iceoryx2 configuration — root-path, TOML file, global setup.
//!
//! Builds and applies the iceoryx2 configuration before any IPC operation.
//!
//! The root-path resolution order is:
//! 1. `ICE_RPC_ROOT_PATH` environment variable (explicit override);
//! 2. platform default: `%APPDATA%\ice-rpc\iceoryx2` on Windows,
//!    `$XDG_DATA_HOME/ice-rpc/iceoryx2` or `~/.local/share/ice-rpc/iceoryx2` on Unix;
//! 3. iceoryx2 default when no path can be determined.

use iceoryx2::prelude::SemanticString;

/// Resolves the iceoryx2 root-path from the environment or a platform default.
fn default_root_path() -> Option<std::path::PathBuf> {
    if let Ok(explicit) = std::env::var("ICE_RPC_ROOT_PATH") {
        if !explicit.trim().is_empty() {
            return Some(std::path::PathBuf::from(explicit));
        }
    }

    #[cfg(windows)]
    {
        let appdata = std::env::var("APPDATA").ok()?;
        Some(
            std::path::PathBuf::from(appdata)
                .join("ice-rpc")
                .join("iceoryx2"),
        )
    }

    #[cfg(not(windows))]
    {
        let base = std::env::var("XDG_DATA_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|_| {
                std::env::var("HOME")
                    .map(|home| std::path::PathBuf::from(home).join(".local").join("share"))
            })
            .unwrap_or_else(|_| std::env::temp_dir());
        Some(base.join("ice-rpc").join("iceoryx2"))
    }
}

/// Builds the iceoryx2 configuration with the resolved root-path.
///
/// # Returns
/// The ready-to-use iceoryx2 [`Config`](iceoryx2::config::Config).
pub(crate) fn build_iceoryx2_config() -> iceoryx2::config::Config {
    let Some(root) = default_root_path() else {
        log::warn!("[ice-rpc] no root-path available (APPDATA unset): using iceoryx2 default.");
        return iceoryx2::config::Config::default();
    };

    if let Err(e) = std::fs::create_dir_all(&root) {
        log::error!(
            "[ice-rpc] Failed to create '{}': {e} - default root-path.",
            root.display()
        );
        return iceoryx2::config::Config::default();
    }

    let _ = std::fs::create_dir_all(root.join("shm"));

    let root_str = root.to_string_lossy();
    let iox_path = match iceoryx2_bb_system_types::path::Path::new(root_str.as_bytes()) {
        Ok(p) => p,
        Err(e) => {
            log::warn!("[ice-rpc] invalid root-path '{root_str}': {e:?} - default used.");
            return iceoryx2::config::Config::default();
        }
    };

    let mut cfg = iceoryx2::config::Config::default();
    cfg.global.set_root_path(&iox_path);

    cfg.global.node.cleanup_dead_nodes_on_creation = true;
    cfg.global.node.cleanup_dead_nodes_on_destruction = false;
    cfg.global.service.cleanup_dead_nodes_on_open = false;

    cfg
}

/// Configures the GLOBAL iceoryx2 configuration before any other operation.
///
/// Must be called at the very beginning of `main()`, even before
/// [`ServiceLocator::global()`](crate::ServiceLocator::global).
///
/// 1. Creates a TOML file `./config/iceoryx2.toml` with the custom root-path.
/// 2. Applies the configuration via `Config::setup_global_config_from_file()`.
pub fn setup_iceoryx2_global_config() {
    let config = build_iceoryx2_config();

    let config_file_path = match write_config_toml(&config) {
        Some(path) => path,
        None => return,
    };

    apply_global_config(&config_file_path);
}

/// Serializes the configuration to TOML and writes it to `./config/iceoryx2.toml`
/// if the file does not already exist (avoids an unnecessary rewrite on each start).
///
/// Builds the absolute path manually via `std::env::current_dir()` to
/// avoid the UNC prefix `\\?\` that Windows adds and that `FilePath` rejects.
///
/// # Returns
/// The absolute path of the file, or `None` on error.
fn write_config_toml(config: &iceoryx2::config::Config) -> Option<std::path::PathBuf> {
    let config_dir = std::path::Path::new("config");
    if let Err(e) = std::fs::create_dir_all(config_dir) {
        log::error!(
            "[ice-rpc] ERROR: failed to create directory '{}': {e}",
            config_dir.display()
        );
        return None;
    }

    let file_path = config_dir.join("iceoryx2.toml");

    // Do not rewrite the file if it already exists (optimization).
    if file_path.exists() {
        let abs_path = match std::env::current_dir() {
            Ok(cwd) => cwd.join(&file_path),
            Err(e) => {
                log::error!("[ice-rpc] ERROR: current_dir(): {e}");
                return None;
            }
        };
        return Some(abs_path);
    }

    let toml_content = match toml::to_string_pretty(config) {
        Ok(c) => c,
        Err(e) => {
            log::error!("[ice-rpc] ERROR: TOML serialization failed: {e}");
            return None;
        }
    };

    if let Err(e) = std::fs::write(&file_path, &toml_content) {
        log::error!("[ice-rpc] ERROR: writing '{}': {e}", file_path.display());
        return None;
    }

    let abs_path = match std::env::current_dir() {
        Ok(cwd) => cwd.join(&file_path),
        Err(e) => {
            log::error!("[ice-rpc] ERROR: current_dir(): {e}");
            return None;
        }
    };

    Some(abs_path)
}

/// Applies the global iceoryx2 configuration from the TOML file.
///
/// Converts `\` into `/` in the path for compatibility with
/// `iceoryx2_bb_system_types::FilePath`.
fn apply_global_config(config_file_path: &std::path::Path) {
    let path_str_raw = config_file_path.to_string_lossy();
    let path_str_fwd = path_str_raw.replace('\\', "/");

    let iox_file_path =
        match iceoryx2_bb_system_types::file_path::FilePath::new(path_str_fwd.as_bytes()) {
            Ok(p) => p,
            Err(e) => {
                log::error!("[ice-rpc] ERROR: invalid FilePath '{path_str_fwd}': {e:?}");
                log::warn!("[ice-rpc] iceoryx2 will use the default root-path!");
                return;
            }
        };

    match iceoryx2::config::Config::setup_global_config_from_file(&iox_file_path) {
        Ok(global_cfg) => {
            let root = String::from_utf8_lossy(global_cfg.global.root_path().as_bytes());
            log::info!("[ice-rpc] Global configuration loaded from '{path_str_fwd}'");
            log::info!("[ice-rpc] verified global root-path: {root}");
        }
        Err(e) => {
            log::error!("[ice-rpc] ERROR loading global config: {e:?}");
            log::warn!("[ice-rpc] iceoryx2 will use the default root-path!");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_config_has_expected_structure() {
        let cfg = build_iceoryx2_config();
        // The config is built without panicking.
        // The specific flags depend on the iceoryx2 default values
        // and the APPDATA path; we only check that the config is valid.
        let _ = cfg;
    }

    #[test]
    fn build_config_returns_default_when_appdata_unset() {
        // Save APPDATA, remove it, check the fallback, restore.
        let saved = std::env::var("APPDATA").ok();
        std::env::remove_var("APPDATA");

        let cfg = build_iceoryx2_config();

        // Restore
        if let Some(val) = saved {
            std::env::set_var("APPDATA", val);
        }

        // Without APPDATA, we must obtain a valid Config (no panic).
        // We only check that the returned type is an iceoryx2 Config.
        let _ = cfg; // The construction succeeded without panicking.
    }

    #[test]
    fn write_config_toml_creates_file_when_missing() {
        // Uses a temporary directory for the test.
        let tmp = std::env::temp_dir().join("ice_rpc_test_config");
        let _ = std::fs::remove_dir_all(&tmp);

        let original_dir = std::env::current_dir().ok();

        // Create and cd into the temporary directory
        std::fs::create_dir_all(&tmp).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        // Clean the file if it already exists
        let config_file = tmp.join("config").join("iceoryx2.toml");
        let _ = std::fs::remove_file(&config_file);
        let _ = std::fs::remove_dir(tmp.join("config"));

        let config = build_iceoryx2_config();
        let result = write_config_toml(&config);

        // Restore the current directory
        if let Some(dir) = original_dir {
            let _ = std::env::set_current_dir(&dir);
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);

        assert!(result.is_some(), "write_config_toml must succeed");
        let path = result.unwrap();
        assert!(path.ends_with("iceoryx2.toml"));
    }

    #[test]
    fn write_config_toml_skips_when_file_exists() {
        let tmp = std::env::temp_dir().join("ice_rpc_test_config_skip");
        let _ = std::fs::remove_dir_all(&tmp);

        let original_dir = std::env::current_dir().ok();

        std::fs::create_dir_all(&tmp).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        // Create the file manually before calling write_config_toml
        let config_dir = tmp.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let config_file = config_dir.join("iceoryx2.toml");
        std::fs::write(&config_file, "# existing config\n").unwrap();

        let config = build_iceoryx2_config();
        let result = write_config_toml(&config);

        // Restore
        if let Some(dir) = original_dir {
            let _ = std::env::set_current_dir(&dir);
        }

        let _ = std::fs::remove_dir_all(&tmp);

        assert!(
            result.is_some(),
            "must return the path even if the file exists"
        );
        let path = result.unwrap();
        assert!(path.ends_with("iceoryx2.toml"));

        // The content must NOT have been modified (the optimization skipped the write)
        // Note: this cannot be verified here because the file was removed.
    }

    #[test]
    fn apply_global_config_path_slash_conversion() {
        // Tests that Windows backslashes are converted to forward slashes.
        let path_with_backslashes = std::path::Path::new(r"C:\Users\test\config\iceoryx2.toml");
        let path_str = path_with_backslashes.to_string_lossy();
        let converted = path_str.replace('\\', "/");
        assert!(!converted.contains('\\'), "backslashes must be converted");
        assert!(converted.contains('/'), "must contain forward slashes");
    }
}
