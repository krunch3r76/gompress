#!/bin/bash
# authored by krunch3r (https://www.github.com/krunch3r76)
TARGET_FILE="$1"
EXPECTED_FILE="$1.xz"

ARGS="$TARGET_FILE $ARGS"
/usr/bin/xz $ARGS

if [[ $? == 0 ]]; then
    echo "OK:$(stat -c %s $EXPECTED_FILE)"
else
    echo "ERROR"
    exit 1
fi

