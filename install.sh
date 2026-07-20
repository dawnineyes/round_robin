#!/bin/bash
set -e

REPO="dawnineyes/round_robin"
INSTALL_DIR="/opt/round_robin"
BINARY="round_robin"

echo "=== round_robin installer ==="

# Fetch latest release info
echo "Fetching latest release..."
RELEASE=$(curl -sSf "https://api.github.com/repos/${REPO}/releases/latest")
TAG=$(echo "$RELEASE" | grep -o '"tag_name": *"[^"]*"' | head -1 | sed 's/.*"\(.*\)".*/\1/')
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${BINARY}"

if [ -z "$TAG" ]; then
    echo "ERROR: failed to fetch latest release"
    exit 1
fi
echo "Latest version: $TAG"

# Download binary
echo "Downloading $DOWNLOAD_URL ..."
sudo mkdir -p "$INSTALL_DIR"
sudo curl -sSfL "$DOWNLOAD_URL" -o "${INSTALL_DIR}/${BINARY}"
sudo chmod +x "${INSTALL_DIR}/${BINARY}"

echo "Installed to ${INSTALL_DIR}/${BINARY}"

# Create systemd service if it doesn't exist
SERVICE_FILE="/etc/systemd/system/round_robin.service"
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
    echo "Service created. Run: sudo systemctl enable --now round_robin"
else
    echo "Systemd service already exists, restarting..."
    sudo systemctl restart round_robin
fi

echo ""
echo "=== Done ==="
echo "Config: ${INSTALL_DIR}/config.toml"
echo "Logs:   journalctl -u round_robin -f"
echo "Status: sudo systemctl status round_robin"
