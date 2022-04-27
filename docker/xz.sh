#!/bin/bash
# authored by krunch3r (https://www.github.com/krunch3r76)
# pre: script command is run from workdir as defined in dockerfile
TARGET_FILE="$1"
NAMESTEM="$(echo $1 | sed -E 's/([^.]+)(\.xz)?/\1/')"
OUTPUT_DIR="/golem/output"
OUTPUT_FILEPATH="$OUTPUT_DIR/$NAMESTEM.xz"

shopt -s nocasematch
REGEX="\.xz$"
if [[ $TARGET_FILE =~ $REGEX ]]; then
    xz -d --stdout $TARGET_FILE | /usr/bin/xz $ARGS --to-stdout >$OUTPUT_FILEPATH
else
    cat $TARGET_FILE | /usr/bin/xz $ARGS --to-stdout >$OUTPUT_FILEPATH
fi

if [[ $? == 0 ]]; then
    echo "OK:$(stat -c %s $OUTPUT_FILEPATH)"
else
    echo "ERROR"
    exit 1
fi
