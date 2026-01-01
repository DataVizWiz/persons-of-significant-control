#!/usr/bin/env bash
set -e

PYTHON_VERSION="3.14"
PYTHON_BIN="python${PYTHON_VERSION}"
INSTALL_PREFIX="/usr/local/python${PYTHON_VERSION}"
BUILD_DIR="/tmp/python${PYTHON_VERSION}-build"

echo "=== Updating package lists ==="
sudo apt update

# Try PPA install first (Ubuntu 22.04/24.04)
if command -v add-apt-repository >/dev/null 2>&1; then
    echo "Adding deadsnakes PPA (if available)..."
    sudo add-apt-repository -y ppa:deadsnakes/ppa || true
    sudo apt update

    echo "Attempting APT install of Python $PYTHON_VERSION..."
    if sudo apt install -y "${PYTHON_BIN}" "${PYTHON_BIN}-venv" "${PYTHON_BIN}-dev"; then
        echo "Python $PYTHON_VERSION installed via apt!"
        PYTHON_PATH="$(which $PYTHON_BIN)"
        goto_set_default=true
    else
        echo "APT install failed or package not available; building from source."
        goto_set_default=false
    fi
else
    goto_set_default=false
fi

# Build from source if needed
if [ "$goto_set_default" = false ]; then
    echo "=== Installing build dependencies ==="
    sudo apt install -y build-essential zlib1g-dev libncurses-dev libgdbm-dev \
        libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev \
        libbz2-dev liblzma-dev uuid-dev tk-dev wget curl

    echo "=== Creating build directory $BUILD_DIR ==="
    sudo rm -rf "$BUILD_DIR"
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    echo "=== Downloading Python $PYTHON_VERSION source ==="
    PY314_VERSION="$(curl -fsSL https://www.python.org/ftp/python/ | grep -oE '3\.14\.[0-9]+' | sort -V | tail -1)"
    if [ -z "$PY314_VERSION" ]; then
        echo "Unable to find a Python 3.14.x release; exiting."
        exit 1
    fi
    echo "Found Python version $PY314_VERSION"
    wget "https://www.python.org/ftp/python/${PY314_VERSION}/Python-${PY314_VERSION}.tar.xz"
    tar -xf "Python-${PY314_VERSION}.tar.xz"
    cd "Python-${PY314_VERSION}"

    echo "=== Configuring build ==="
    ./configure --prefix="$INSTALL_PREFIX" --enable-optimizations --with-ensurepip=install

    echo "=== Building Python (this may take several minutes) ==="
    make -j"$(nproc)"

    echo "=== Installing Python $PYTHON_VERSION ==="
    sudo make altinstall

    PYTHON_PATH="$INSTALL_PREFIX/bin/python3.14"
    echo "Python $PYTHON_VERSION installed at $PYTHON_PATH"
fi

# === Set Python 3.14 as default for python3 safely ===
echo "=== Configuring update-alternatives for python3 ==="
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3 1 2>/dev/null || true
sudo update-alternatives --install /usr/bin/python3 python3 "$PYTHON_PATH" 2
sudo update-alternatives --set python3 "$PYTHON_PATH"

echo "Python3 default version is now:"
python3 --version

# Set 'python' to point to python3
echo "=== Configuring 'python' command to point to python3 ==="
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1 2>/dev/null || true
sudo update-alternatives --set python /usr/bin/python3

echo "Python command now points to:"
python --version

# Ensure pip is installed and upgraded
echo "=== Verifying pip for Python 3.14 ==="
"$PYTHON_PATH" -m ensurepip --upgrade
"$PYTHON_PATH" -m pip install --upgrade pip
echo "pip version: $("$PYTHON_PATH" -m pip --version)"

# === Clean up build files safely with sudo to avoid permission denied errors ===
if [ -d "$BUILD_DIR" ]; then
    echo "=== Cleaning up build directory $BUILD_DIR ==="
    sudo rm -rf "$BUILD_DIR"
fi

echo "=== Installation and configuration complete! ==="
