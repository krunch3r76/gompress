#!/bin/bash
# authored by krunch3r (https://www.github.com/krunch3r76)
TARGET_FILE="$1"
EXPECTED_FILE="$1.xz"
shift
CHECKSUMARG=$1
shift
if [[ "$CHECKSUMARG" == "checksum" ]]; then
    ENABLE_CHECKSUM=1
    ARGS="$@"
else
    ENABLE_CHECKSUM=0
    ARGS="$CHECKSUMARG $@"
fi

ARGS="$TARGET_FILE $ARGS"
/usr/bin/xz $ARGS

if [[ $? == 0 ]]; then
    if [[ $ENABLE_CHECKSUM -eq 1 ]]; then
	echo "OK:$(sha1sum $EXPECTED_FILE)" | cut -f1 -d " "
    else
	echo $EXPECTED_FILE
	echo "OK:$(stat -c %s $EXPECTED_FILE)"
    fi
else
    echo "ERROR"
fi

