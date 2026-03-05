#!/bin/bash

# This script builds HammerDB from source and applies necessary patches.

if [ ! -f "hammerdbcli" ]; then
    echo "Error: Run this script from within the HammerDB source directory."
    exit 1
fi

pushd Build/Bawt-2.1.0

echo "Building HammerDB from source for pg only."
echo "--> Commenting out other database drivers in Setup/HammerDB-Linux.bawt"
sed -i -E 's/^(Setup (ora|mariatcl|oratcl|mysqltcl|db2tcl))/#\1/' Setup/HammerDB-Linux.bawt

./Build-Linux.sh x64 Setup/HammerDB-Linux.bawt complete --list

popd
