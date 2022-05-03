#!/bin/bash
# authored by krunch3r (https://www.github.com/krunch3r76)
# pre: script command is run from workdir as defined in dockerfile
TARGET_FILE="$1"
NAMESTEM=$(basename $TARGET_FILE .xz)

OUTPUT_DIR="/golem/output"
OUTPUT_FILEPATH="$OUTPUT_DIR/$NAMESTEM.xz"

shift
ARGS="$@"

shopt -s nocasematch
REGEX="\.xz$"
if [[ $TARGET_FILE =~ $REGEX ]]; then
    CMD="xz -d --stdout $TARGET_FILE | /usr/bin/xz $ARGS --to-stdout >$OUTPUT_FILEPATH"
else
    CMD="cat $TARGET_FILE | /usr/bin/xz $ARGS --to-stdout >$OUTPUT_FILEPATH"
fi
/usr/bin/time -v -o $OUTPUT_DIR/${NAMESTEM}.tim bash -c "$CMD"
if [[ $? == 0 ]]; then
    echo -n "OK-$(stat -c %s $OUTPUT_FILEPATH)-"
    cat $OUTPUT_DIR/${NAMESTEM}.tim | grep "Elapsed" | sed -En 's/(.*): (.*)$/\2/p'
else
    echo "ERROR"
    exit 1
fi
