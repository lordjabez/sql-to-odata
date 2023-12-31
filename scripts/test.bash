#!/usr/bin/env bash
set -e

pytest -vv --cov=sql_to_odata --cov-report=term --cov-report=xml

flake8 --max-line-length=120 --statistics sql_to_odata
flake8 --max-line-length=120 --statistics tests

bandit -r sql_to_odata
bandit -s B101 -r tests

# safety check --full-report

echo
echo "All tests passed successfully!"
