#!/usr/bin/env bash
# setup_termshot.sh — Install charmbracelet/freeze + Pillow fallback
set -euo pipefail

echo "=== Setup Termshot: freeze + Pillow ==="

# Install Pillow (Python fallback)
pip install Pillow 2>/dev/null && echo "  ✅ Pillow installed" || echo "  ⚠️  Pillow install failed"

# Try to install freeze binary
FREEZE_URL="https://github.com/charmbracelet/freeze/releases/latest/download/freeze"
FREEZE_BIN="/usr/local/bin/freeze"

if command -v freeze &>/dev/null; then
    echo "  ✅ freeze already installed: $(freeze --version 2>/dev/null || echo 'ok')"
elif command -v curl &>/dev/null; then
    echo "  🔽 Downloading freeze..."
    if curl -sL "$FREEZE_URL" -o /tmp/freeze 2>/dev/null; then
        chmod +x /tmp/freeze
        if [ -w /usr/local/bin ]; then
            mv /tmp/freeze "$FREEZE_BIN"
        else
            sudo mv /tmp/freeze "$FREEZE_BIN" 2>/dev/null || {
                mkdir -p ~/.local/bin
                mv /tmp/freeze ~/.local/bin/freeze
                echo "  ⚠️  Installed to ~/.local/bin — add to PATH"
            }
        fi
        echo "  ✅ freeze installed"
    else
        echo "  ⚠️  Could not download freeze — will use Pillow fallback"
    fi
else
    echo "  ⚠️  curl not available — will use Pillow fallback"
fi

echo "=== Setup complete ==="
