use flate2::read::GzDecoder;
use log::info;
use std::env;
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

const LIBHDFS_BASE_URL: &str = "https://repo.hops.works/master/libhdfs";
const LIB_DIR_ENV: &str = "HDFS_LIB_DIR";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("cargo:rerun-if-env-changed={}", LIB_DIR_ENV);
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    println!("cargo:rerun-if-changed=HOPS_VERSION");
    println!("cargo:rerun-if-changed=build.rs");

    // docs.rs builds in an offline sandbox and only runs `cargo doc`, which
    // never invokes the linker — so we skip downloading libhdfs and emitting
    // link directives entirely.
    if env::var("DOCS_RS").is_ok() {
        return Ok(());
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let lib_dir = resolve_lib_dir(&out_dir)?;
    set_libraries(&out_dir, &lib_dir);
    Ok(())
}

fn resolve_lib_dir(out_dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(dir) = env::var(LIB_DIR_ENV) {
        let dir = PathBuf::from(dir);
        if !dir.is_dir() {
            return Err(format!("{}={:?} is not a directory", LIB_DIR_ENV, dir).into());
        }
        info!("Using prebuilt libhdfs from {:?}", dir);
        return Ok(dir);
    }

    let lib_dir = out_dir.join("lib");
    download_and_extract_libhdfs(&lib_dir)?;
    Ok(lib_dir)
}

fn download_and_extract_libhdfs(lib_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let version = fs::read_to_string("HOPS_VERSION")
        .expect("Failed to read HOPS_VERSION file")
        .trim()
        .to_string();

    let tarball_url = format!("{}/libhdfs-golang-{}.tar.gz", LIBHDFS_BASE_URL, version);
    info!("Downloading libhdfs-golang from {}", tarball_url);

    let client = reqwest::blocking::Client::new();
    let response = client.get(&tarball_url).send()?;

    if !response.status().is_success() {
        return Err(format!(
            "Failed to download libhdfs-golang: HTTP {}",
            response.status()
        )
        .into());
    }

    if lib_dir.exists() {
        fs::remove_dir_all(lib_dir)?;
    }
    fs::create_dir_all(lib_dir)?;

    let decoder = GzDecoder::new(response);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;

        if !entry.header().entry_type().is_file() {
            continue;
        }

        let path = entry.path()?.to_path_buf();
        let file_name = path
            .file_name()
            .ok_or("Invalid file name in archive")?
            .to_str()
            .ok_or("Non-UTF8 file name")?
            .to_string();

        let dest_path = lib_dir.join(&file_name);
        let mut dest_file = File::create(&dest_path)?;
        io::copy(&mut entry, &mut dest_file)?;

        info!("Extracted: {}", file_name);
    }

    info!("Successfully extracted libhdfs-golang to {:?}", lib_dir);
    Ok(())
}

// Build scripts run on the host: read the target OS from cargo, so a cross build links
// the target's library.
fn set_libraries(out_dir: &Path, lib_dir: &Path) {
    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") => {
            create_symlinks(out_dir, lib_dir, "macos", false);
            println!("cargo:rustc-link-search=native={}", out_dir.display());
            println!("cargo:rustc-link-lib=static=hdfs");
            println!("cargo:rustc-link-lib=framework=Security");
            println!("cargo:rustc-link-lib=framework=CoreFoundation");
            println!("cargo:rustc-link-lib=resolv");
        }
        Ok("linux") => {
            create_symlinks(out_dir, lib_dir, "linux", true);
            println!("cargo:rustc-link-search=native={}", out_dir.display());
            println!("cargo:rustc-link-lib=hdfs");
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
        }
        other => panic!(
            "Unsupported target OS {other:?}: HopsFS object store only supports macOS and Linux."
        ),
    }
}

fn create_symlinks(out_dir: &Path, lib_dir: &Path, target_os: &str, shared: bool) {
    let filter = match target_os {
        "linux" => "linux-amd64",
        "macos" => "arm64",
        other => panic!("Unsupported target OS: {}", other),
    };

    let lib_ext = if shared {
        match target_os {
            "linux" => ".so",
            "macos" => ".dylib",
            other => panic!("Unsupported target OS: {}", other),
        }
    } else {
        ".a"
    };

    let mut lib_file = None;
    let mut header_file = None;

    for entry in fs::read_dir(lib_dir).expect("Could not read lib directory") {
        let entry = entry.expect("Error reading directory entry");
        let file_name = entry
            .file_name()
            .into_string()
            .expect("Invalid file name");

        if file_name.ends_with(lib_ext) && file_name.contains(filter) {
            lib_file = Some(entry.path());
        } else if file_name.ends_with(".h") && file_name.contains(filter) {
            header_file = Some(entry.path());
        }
    }

    let lib_file = lib_file.expect("Library file not found");
    let header_file = header_file.expect("Header file not found");

    let symlink_lib = out_dir.join(format!("libhdfs{}", lib_ext));
    let symlink_header = out_dir.join("libhdfs.h");

    if symlink_lib.symlink_metadata().is_ok() {
        fs::remove_file(&symlink_lib).expect("Failed to remove existing library symlink");
    }
    if symlink_header.symlink_metadata().is_ok() {
        fs::remove_file(&symlink_header).expect("Failed to remove existing header symlink");
    }

    symlink(&lib_file, &symlink_lib).expect("Failed to create symlink for library");
    symlink(&header_file, &symlink_header).expect("Failed to create symlink for header");

    info!(
        "Created symlinks: {:?} -> {:?}, {:?} -> {:?}",
        symlink_lib, lib_file, symlink_header, header_file
    );
}
