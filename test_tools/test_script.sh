#!/bin/bash
# Example test script for the precipitation analyzer

# set -e

echo "Building precip-analyzer..."
cargo build --release

echo ""
echo "Running example queries..."
echo ""

echo "=== Example 1: Seattle next 7 days with ensemble ==="
cargo run --release -- \
  --city "Seattle, WA" \
  --start 2026-02-09 \
  --end 2026-02-16 \
  --unit inch

echo ""
echo "=== Example 2: New York with verbose output ==="
cargo run --release -- \
  --city "New York" \
  --start 2026-02-09 \
  --end 2026-02-13 \
  --verbose

echo ""
echo "=== Example 3: Coordinates (Miami) ==="
cargo run --release -- \
  --lat 25.7617 --lon '-80.1918' \
  --start 2026-02-09 \
  --end 2026-02-16

echo ""
echo "All examples completed successfully!"
