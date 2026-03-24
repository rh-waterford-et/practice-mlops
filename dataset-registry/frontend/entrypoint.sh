#!/bin/sh
cat > /opt/app-root/src/config.js <<EOF
window.MARQUEZ_WEB_URL = "${MARQUEZ_WEB_URL:-}";
EOF
exec nginx -g "daemon off;"
