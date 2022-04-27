#!/bin/bash
# authored by krunch3r (https://www.github.com/krunch3r76)
# pre: script command is run from workdir as defined in dockerfile
TARGET_FILE="$1"
OUTPUT_FILE="output.xz"
shopt -s nocasematch
REGEX="\.xz$"
if [[ $TARGET_FILE =~ $REGEX ]]; then
    # cat $TARGET_FILE | xz -d --to-stdout | /usr/bin/xz $ARGS >$OUTPUT_FILE
    xz -d --stdout $TARGET_FILE | /usr/bin/xz $ARGS --to-stdout >$OUTPUT_FILE
else
    cat $TARGET_FILE | /usr/bin/xz $ARGS --to-stdout >$OUTPUT_FILE
fi

if [[ $? == 0 ]]; then
    echo "OK:$(stat -c %s $OUTPUT_FILE)"
else
    echo "ERROR"
    exit 1
fi
