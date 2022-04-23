#!/bin/bash
# authored by krunch3r (https://www.github.com/krunch3r76)
EXPECTED_FILE="$1.xz"
/usr/bin/xz "$@"
if [[ $? == 0 ]]; then
    echo "OK:$(sha1sum $EXPECTED_FILE)" | cut -f1 -d " "
else
    echo "ERROR"
fi

