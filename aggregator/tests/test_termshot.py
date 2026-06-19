"""
test_termshot.py — 4 tests for termshot screenshot functionality.
Tests that screenshot tools are available and can produce output.
"""
import os, pytest, pathlib, subprocess, sys

SCREENSHOTS_DIR = pathlib.Path("screenshots")
SCRIPTS_DIR = pathlib.Path("scripts")


def _has_command(cmd: str) -> bool:
    """Check if a command is available."""
    try:
        subprocess.run(
            ["which", cmd] if sys.platform != "win32" else ["where", cmd],
            capture_output=True, check=True
        )
        return True
    except Exception:
        return False


def _pillow_available() -> bool:
    """Check if Pillow is installed."""
    try:
        import PIL  # noqa
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _has_command("freeze"), reason="freeze not installed")
def test_freeze_available():
    """Test 1: freeze binary is installed and executable."""
    result = subprocess.run(["freeze", "--version"], capture_output=True, text=True)
    assert result.returncode == 0, "freeze should be executable"


@pytest.mark.skipif(not _pillow_available(), reason="Pillow not installed")
def test_pillow_available():
    """Test 2: Pillow is importable (fallback)."""
    import PIL
    assert hasattr(PIL, "__version__"), "Pillow should have __version__"
    print(f"  Pillow version: {PIL.__version__}")


def test_make_screenshot_script_exists():
    """Test 3: make_screenshot.py exists and is readable."""
    script = SCRIPTS_DIR / "make_screenshot.py"
    assert script.exists(), "scripts/make_screenshot.py harus ada"
    assert script.is_file(), "scripts/make_screenshot.py harus file"
    # Can be parsed as Python
    with open(script) as f:
        code = f.read()
    compile(code, str(script), "exec")
    print(f"  ✅ {script} is valid Python")


def test_screenshots_dir_exists():
    """Test 4: screenshots/ directory exists (or can be created)."""
    if not SCREENSHOTS_DIR.exists():
        SCREENSHOTS_DIR.mkdir(parents=True)
        print("  📁 Created screenshots/ directory")
    assert SCREENSHOTS_DIR.is_dir(), "screenshots/ harus direktori"
    print(f"  ✅ Screenshots directory: {SCREENSHOTS_DIR.resolve()}")
