//! The `values` CLI contract (#101): print the effective values the controller
//! would apply, without touching a cluster, and exit 2 on usage errors.

use std::process::{Command, Output};

use serde_json::Value;
use stardelt_operator::crd::Profile;
use stardelt_operator::values::effective_values;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_stardelt-operator"))
        .args(args)
        .output()
        .expect("run the stardelt-operator binary")
}

fn parse_stdout(output: &Output) -> Value {
    let stdout = std::str::from_utf8(&output.stdout).expect("stdout is UTF-8");
    serde_yaml::from_str(stdout).unwrap_or_else(|e| panic!("stdout is not YAML: {e}\n{stdout}"))
}

#[test]
fn prints_the_effective_values_for_every_profile() {
    for (profile, arg) in [
        (Profile::Demo, "demo"),
        (Profile::Full, "full"),
        (Profile::Airgap, "airgap"),
    ] {
        let output = run(&["values", "jupyterhub", arg]);
        assert!(
            output.status.success(),
            "values jupyterhub {arg} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            parse_stdout(&output),
            effective_values("jupyterhub", profile, None, None),
            "profile {arg} differs from effective_values"
        );
        assert!(
            output.stdout.ends_with(b"\n"),
            "YAML document ends with a newline"
        );
    }
}

#[test]
fn an_unknown_component_prints_an_empty_document() {
    let output = run(&["values", "nope", "demo"]);
    assert!(output.status.success(), "unknown component is not an error");
    assert_eq!(parse_stdout(&output), serde_json::json!({}));
}

#[test]
fn missing_arguments_are_a_usage_error() {
    let output = run(&["values", "jupyterhub"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        output.stdout.is_empty(),
        "usage errors print nothing to stdout"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("usage: stardelt-operator values"),
        "stderr explains the usage: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn an_unknown_profile_is_a_usage_error() {
    let output = run(&["values", "jupyterhub", "bogus"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        output.stdout.is_empty(),
        "usage errors print nothing to stdout"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("<demo|full|airgap>"),
        "stderr lists the profiles: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
