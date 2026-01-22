#!/bin/bash

# Database Connection Validation Script
# Validates all database connections configured in the profiles

set -e

cd "$(dirname "$0")"

echo "Running database connection validation..."
echo ""

dotnet run --project TestDbConnections/TestDbConnections.csproj --verbosity quiet

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "✅ All database connections are valid!"
    echo ""
    echo "See CHECK_CONNECTIONS.md for detailed validation report"
else
    echo ""
    echo "❌ Some database connections failed!"
    echo ""
    echo "Please check the output above for details"
fi

exit $exit_code
