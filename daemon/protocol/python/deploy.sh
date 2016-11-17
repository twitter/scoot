#/bin/bash
#
# Run integration test and if successful, upload the latest code to pypi.

set -e

echo "See README.md for all required setup. Continuing..."

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
go test ../../integration/python
python setup.py sdist upload
