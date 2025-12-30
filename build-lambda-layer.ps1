# ==========================
# CONFIGURATION
# ==========================
$PythonVersion = "python3.14"
$LayerRoot     = "layer"
$SitePackages  = "$LayerRoot/python/lib/$PythonVersion/site-packages"
$ZipName       = "lambda-layer.zip"

# ==========================
# SAFETY CHECKS
# ==========================
if (-not (Get-Command pip -ErrorAction SilentlyContinue)) {
    Write-Error "pip not found. Make sure Python is installed and pip is on PATH."
    exit 1
}

if (-not (Test-Path "requirements.txt")) {
    Write-Error "requirements.txt not found in current directory."
    exit 1
}

# ==========================
# CLEAN PREVIOUS BUILD
# ==========================
Write-Host "Cleaning previous build..."
Remove-Item -Recurse -Force $LayerRoot, $ZipName -ErrorAction SilentlyContinue

# ==========================
# CREATE DIRECTORY STRUCTURE
# ==========================
Write-Host "Creating Lambda layer directory structure..."
New-Item -ItemType Directory -Force -Path $SitePackages | Out-Null

# ==========================
# INSTALL DEPENDENCIES
# ==========================
Write-Host "Installing dependencies into layer..."
pip install `
    --upgrade `
    --no-cache-dir `
    -r requirements.txt `
    --target $SitePackages

if ($LASTEXITCODE -ne 0) {
    Write-Error "pip install failed."
    exit 1
}

# ==========================
# CLEAN UNNECESSARY FILES
# ==========================
Write-Host "Removing unnecessary files..."

# Remove __pycache__
Get-ChildItem $LayerRoot -Recurse -Directory -Filter "__pycache__" |
    Remove-Item -Recurse -Force

# Remove tests
Get-ChildItem $SitePackages -Recurse -Directory |
    Where-Object { $_.Name -match "test|tests" } |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Remove dist-info metadata (safe for Lambda)
Get-ChildItem $SitePackages -Recurse -Directory -Filter "*.dist-info" |
    Remove-Item -Recurse -Force

# Remove compiled artifacts not needed at runtime
Get-ChildItem $SitePackages -Recurse -Include "*.pyc","*.pyo","*.whl" |
    Remove-Item -Force -ErrorAction SilentlyContinue

# ==========================
# CREATE ZIP
# ==========================
Write-Host "Creating zip file..."

Push-Location $LayerRoot
Compress-Archive -Path "python" -DestinationPath "../$ZipName"
Pop-Location

# ==========================
# DONE
# ==========================
Write-Host ""
Write-Host "Lambda layer build complete!"
Write-Host "Output: $ZipName"
Write-Host "Python runtime: $PythonVersion"
