//! `k9 run --tui` from the command line. The dashboard itself is tested in
//! k9-tui; a real terminal session is in `tests/python/test_tui.py`.

use std::path::PathBuf;
use std::process::Command;

fn plan(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("k9-cli-tui-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.yaml"));
    std::fs::write(
        &path,
        "options: { iterations: 2 }\nflows: { default: [ { sleep: 1ms } ] }\n",
    )
    .unwrap();
    path
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn without_a_terminal_the_usual_output_is_shown() {
    let output = Command::new(env!("CARGO_BIN_EXE_k9"))
        .args(["run", "--tui", &plan("plain").display().to_string()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    assert!(
        text(&output.stderr).contains("--tui needs a terminal"),
        "{}",
        text(&output.stderr)
    );
    assert!(text(&output.stdout).contains("iterations"), "the summary");
}

#[test]
fn an_unknown_theme_is_a_config_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_k9"))
        .args([
            "run",
            "--tui",
            "--theme",
            "pink",
            &plan("theme").display().to_string(),
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "clap's usage error");
    assert!(
        text(&output.stderr).contains("brand or neon"),
        "{}",
        text(&output.stderr)
    );
}
