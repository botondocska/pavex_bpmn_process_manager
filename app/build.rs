use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=static/input.css");
    println!("cargo:rerun-if-changed=templates");
    println!("cargo:rerun-if-env-changed=SKIP_TAILWIND_BUILD");

    if std::env::var("SKIP_TAILWIND_BUILD").is_ok() {
        return;
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");

    let binary_name = if cfg!(target_os = "windows") {
        "tailwindcss.exe"
    } else {
        "tailwindcss"
    };

    let binary_path: PathBuf = std::env::var("TAILWIND_BIN_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| [manifest_dir, "tailwind-bin", binary_name].iter().collect());

    if !binary_path.exists() {
        panic!("tailwindcss binary not found at {:?}", binary_path);
    }

    let input: PathBuf = [manifest_dir, "static", "input.css"].iter().collect();
    let output: PathBuf = [manifest_dir, "static", "output.css"].iter().collect();

    let status = Command::new(&binary_path)
        .arg("-i")
        .arg(&input)
        .arg("-o")
        .arg(&output)
        .arg("--minify")
        .status()
        .expect("failed to spawn tailwindcss");

    if !status.success() {
        panic!("tailwind build failed");
    }
}
