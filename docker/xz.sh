#!/bin/bash
# authored by krunch3r (https://www.github.com/krunch3r76)
TARGET_FILE="$1"
EXPECTED_FILE="$1.xz"

cat $TARGET_FILE | xz -d --to-stdout | /usr/bin/xz $ARGS >$EXPECTED_FILE

if [[ $? == 0 ]]; then
    echo "OK:$(stat -c %s $EXPECTED_FILE)"
else
    echo "ERROR"
    exit 1
fi

