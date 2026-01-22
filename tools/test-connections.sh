#!/bin/bash

# Database Connection Validation Script
# Tests all configured database connections

cd "$(dirname "$0")"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     Database Connection Validation                           ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Check if dotnet is installed
if ! command -v dotnet &> /dev/null; then
    echo "Error: dotnet CLI not found. Please install .NET 8 SDK."
    exit 1
fi

# Build the project if needed
echo "Building project..."
dotnet build DatabaseSync/DatabaseSync.csproj -q

if [ $? -ne 0 ]; then
    echo "Build failed. Please fix compilation errors first."
    exit 1
fi

echo ""
echo "Running connection validation..."
echo ""

# Run the validation utility
cd DatabaseSync
dotnet run --no-build --project DatabaseSync.csproj -- validate-connections

exit $?
