#!/bin/bash
set -e

REPO="dawnineyes/round_robin"
INSTALL_DIR="/opt/round_robin"
BINARY="round_robin"
SERVICE="round_robin"
SERVICE_FILE="/etc/systemd/system/${SERVICE}.service"

echo "=== round_robin installer ==="

# ── Fetch release (pinned version or latest) ───────────────────────────

# Optional positional argument: `install.sh v1.10.4` installs that exact
# release; without it the latest release is installed.
PINNED_TAG="${1:-}"
if [ -n "$PINNED_TAG" ] && ! echo "$PINNED_TAG" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "ERROR: invalid version '$PINNED_TAG' (expected e.g. v1.10.4)"
    exit 1
fi

if [ -n "$PINNED_TAG" ]; then
    echo "Fetching release $PINNED_TAG ..."
    RELEASE=$(curl -sSf "https://api.github.com/repos/${REPO}/releases/tags/${PINNED_TAG}")
else
    echo "Fetching latest release..."
    RELEASE=$(curl -sSf "https://api.github.com/repos/${REPO}/releases/latest")
fi
TAG=$(echo "$RELEASE" | grep -o '"tag_name": *"[^"]*"' | head -1 | sed 's/.*"\(.*\)".*/\1/')
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${BINARY}"

if [ -z "$TAG" ]; then
    echo "ERROR: failed to fetch ${PINNED_TAG:-latest} release (does the release/tag exist?)"
    exit 1
fi
echo "Target version: $TAG"

# ── Stop running service ───────────────────────────────────────────────

if systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
    echo "Stopping running service..."
    sudo systemctl stop "$SERVICE"
    # Wait for the old process to actually die before downloading
    for i in $(seq 1 30); do
        if ! systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
            break
        fi
        sleep 1
    done
fi

# ── Download binary ────────────────────────────────────────────────────

echo "Downloading $DOWNLOAD_URL ..."
sudo mkdir -p "$INSTALL_DIR"
sudo curl -sSfL "$DOWNLOAD_URL" -o "${INSTALL_DIR}/${BINARY}.tmp"
sudo chmod +x "${INSTALL_DIR}/${BINARY}.tmp"
sudo mv "${INSTALL_DIR}/${BINARY}.tmp" "${INSTALL_DIR}/${BINARY}"

echo "Installed to ${INSTALL_DIR}/${BINARY} (${TAG})"

# ── Create or update systemd service ───────────────────────────────────

if [ ! -f "$SERVICE_FILE" ]; then
    echo "Creating systemd service..."
    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=round_robin
After=network.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/${BINARY}
Restart=always
RestartSec=3
# BUG-13: the daemon handles SIGTERM gracefully; give it a bounded window.
KillSignal=SIGTERM
TimeoutStopSec=10

[Install]
WantedBy=multi-user.target
EOF
    sudo systemctl daemon-reload
    sudo systemctl enable "$SERVICE"
fi

# ── Done ────────────────────────────────────────────────────────────────

echo ""
echo "=== Done ==="
echo "Binary: ${INSTALL_DIR}/${BINARY} (${TAG})"
echo "Config: ${INSTALL_DIR}/config.toml"
echo "Run:    sudo systemctl restart ${SERVICE}"
