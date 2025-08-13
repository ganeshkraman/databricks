#!/bin/bash
set -euxo pipefail

# Speed up/avoid tz prompts
export DEBIAN_FRONTEND=noninteractive

# Update package catalog
apt-get update

# Install LibreOffice and fonts (adjust set as needed)
apt-get install -y \
  libreoffice \
  libreoffice-core \
  libreoffice-writer \
  libreoffice-calc \
  fonts-dejavu-core

# Optional helper used by some users for conversions via UNO
/databricks/python/bin/pip install --no-cache-dir unoconv || true

# Make pyuno importable in Python (location varies a bit by image; this works on DBR Ubuntu)
if [ -d /usr/lib/libreoffice/program ]; then
  echo 'export PYTHONPATH=/usr/lib/libreoffice/program:$PYTHONPATH' >> /etc/profile.d/libreoffice.sh
fi
