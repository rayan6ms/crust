use std::path::Path;
use std::process::Command;

fn main() {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR");
    let root = Path::new(&manifest)
        .parent()
        .and_then(Path::parent)
        .expect("crust-server is inside the Crust workspace");
    emit("CRUST_BUILD_COMMIT", git(root, &["rev-parse", "HEAD"]));
    emit(
        "CRUST_BUILD_BRANCH",
        git(root, &["rev-parse", "--abbrev-ref", "HEAD"]),
    );
    emit(
        "CRUST_BUILD_COMMIT_TIME",
        git(root, &["show", "-s", "--format=%ct", "HEAD"]),
    );
    emit(
        "CRUST_BUILD_DIRTY",
        Some(
            git(root, &["status", "--porcelain", "--untracked-files=no"]).map_or_else(
                || "unknown".to_owned(),
                |status| (!status.is_empty()).to_string(),
            ),
        ),
    );
    println!(
        "cargo:rustc-env=CRUST_BUILD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_default()
    );
    println!("cargo:rerun-if-env-changed=CRUST_BUILD_COMMIT");
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/HEAD").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/index").display()
    );
}

fn git(root: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn emit(name: &str, value: Option<String>) {
    println!(
        "cargo:rustc-env={name}={}",
        value.unwrap_or_else(|| "unknown".to_owned())
    );
}
