use std::path::Path;

#[test]
fn should_keep_one_off_automation_out_of_top_level_scripts_directory() {
    // Arrange
    let scripts = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts");

    // Act
    let exists = scripts.exists();

    // Assert
    assert!(
        !exists,
        "top-level scripts directory is not allowed; use Rust tests or explicit workflow steps"
    );
}
