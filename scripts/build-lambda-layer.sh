#!/usr/bin/env bash
set -euo pipefail

# ==========================
# CONFIGURATION
# ==========================
PYTHON_VERSION="python3.14"   # MUST match Lambda runtime
LAYER_ROOT="layer"
SITE_PACKAGES="$LAYER_ROOT/python/lib/$PYTHON_VERSION/site-packages"
ZIP_NAME="lambda-layer.zip"

# ==========================
# HELPER FUNCTIONS
# ==========================
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

install_zip() {
  echo "zip not found. Attempting to install..."

  if command_exists apt-get; then
    sudo apt-get update
    sudo apt-get install -y zip
  elif command_exists yum; then
    sudo yum install -y zip
  elif command_exists dnf; then
    sudo dnf install -y zip
  else
    echo "ERROR: No supported package manager found. Install zip manually." >&2
    exit 1
  fi
}

# ==========================
# SAFETY CHECKS
# ==========================
if ! command_exists pip; then
  echo "ERROR: pip not found. Make sure Python and pip are installed." >&2
  exit 1
fi

if [[ ! -f "requirements.txt" ]]; then
  echo "ERROR: requirements.txt not found in current directory." >&2
  exit 1
fi

if ! command_exists zip; then
  install_zip
fi

# ==========================
# CLEAN PREVIOUS BUILD
# ==========================
echo "Cleaning previous build..."
rm -rf "$LAYER_ROOT" "$ZIP_NAME"

# ==========================
# CREATE DIRECTORY STRUCTURE
# ==========================
echo "Creating Lambda layer directory structure..."
mkdir -p "$SITE_PACKAGES"

# ==========================
# INSTALL DEPENDENCIES
# ==========================
echo "Installing dependencies into layer..."
pip install \
  --upgrade \
  --no-cache-dir \
  -r requirements.txt \
  --target "$SITE_PACKAGES"

# ==========================
# CLEAN UNNECESSARY FILES
# ==========================
echo "Removing unnecessary files..."

# Remove __pycache__
find "$LAYER_ROOT" -type d -name "__pycache__" -prune -exec rm -rf {} +

# Remove test directories
find "$SITE_PACKAGES" -type d \( -iname "test" -o -iname "tests" \) -prune -exec rm -rf {} + || true

# Remove dist-info metadata
find "$SITE_PACKAGES" -type d -name "*.dist-info" -prune -exec rm -rf {} +

# Remove compiled artifacts not needed at runtime
find "$SITE_PACKAGES" -type f \( -name "*.pyc" -o -name "*.pyo" -o -name "*.whl" \) -delete || true

# ==========================
# CREATE ZIP
# ==========================
echo "Creating zip file..."
(
  cd "$LAYER_ROOT"
  zip -r "../$ZIP_NAME" python
)

# ==========================
# DONE
# ==========================
echo ""
echo "Lambda layer build complete!"
echo "Output: $ZIP_NAME"
echo "Python runtime: $PYTHON_VERSION"
