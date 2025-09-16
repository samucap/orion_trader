#!/bin/bash

extractASCII() {
    local hexStr=$1
    if [[ -z "$hexStr" ]]; then
        echo "Please provide a string to parse"
        return 1
    fi

    echo "$hexStr" | xxd -r -p | strings -n 4
}
