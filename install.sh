#!/bin/bash
set -e

REPO="dawnineyes/round_robin"
INSTALL_DIR="/opt/round_robin"
BINARY="round_robin"
SERVICE="round_robin"
SERVICE_FILE="/etc/systemd/system/${SERVICE}.service"

echo "=== round_robin installer ==="

# ── Fetch latest release ───────────────────────────────────────────────

echo "Fetching latest release..."
RELEASE=$(curl -sSf "https://api.github.com/repos/${REPO}/releases/latest")
TAG=$(echo "$RELEASE" | grep -o '"tag_name": *"[^"]*"' | head -1 | sed 's/.*"\(.*\)".*/\1/')
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${BINARY}"

if [ -z "$TAG" ]; then
    echo "ERROR: failed to fetch latest release"
    exit 1
fi
echo "Latest version: $TAG"

# ── Stop running service ───────────────────────────────────────────────

if systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
    echo "Stopping running service..."
    sudo systemctl stop "$SERVICE"
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

[Install]
WantedBy=multi-user.target
EOF
    sudo systemctl daemon-reload
    sudo systemctl enable "$SERVICE"
fi

# ── Start service ──────────────────────────────────────────────────────

echo "Starting service..."
sudo systemctl start "$SERVICE"

echo ""
echo "=== Done ==="
echo "Config: ${INSTALL_DIR}/config.toml"
echo "Logs:   journalctl -u ${SERVICE} -f"
echo "Status: sudo systemctl status ${SERVICE}"
