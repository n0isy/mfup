#!/bin/sh
# Provision the webtop container: install browsers for DnD testing.
# firefox-esr → webkitGetAsEntry path; chromium → getAsFileSystemHandle path.
set -e
docker exec mfup-webtop bash -c '
  apt-get update -qq &&
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq firefox-esr chromium >/dev/null &&
  echo "browsers installed"
'
# Desktop shortcuts pointing at the demo (same compose network → http://caddy)
docker exec mfup-webtop bash -c '
  mkdir -p /config/Desktop
  cat > /config/Desktop/MFUP-demo-firefox.desktop <<EOF
[Desktop Entry]
Type=Application
Name=MFUP demo (Firefox)
Exec=firefox-esr http://caddy/
Icon=firefox-esr
EOF
  cat > /config/Desktop/MFUP-demo-chromium.desktop <<EOF
[Desktop Entry]
Type=Application
Name=MFUP demo (Chromium)
Exec=chromium --no-sandbox http://caddy/
Icon=chromium
EOF
  chmod +x /config/Desktop/*.desktop
  chown -R abc:abc /config/Desktop
  echo "shortcuts created"
'
