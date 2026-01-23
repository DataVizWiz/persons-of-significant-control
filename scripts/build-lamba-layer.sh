source .venv/bin/activate
pip install -r requirements.txt

mkdir -p python/lib
cp -a .venv/lib/* python/lib
zip -r python.zip python
rm -rf python
