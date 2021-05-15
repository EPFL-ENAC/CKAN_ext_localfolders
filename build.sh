#!/bin/bash

set -e

function cleanup {
    exit $?
}

trap "cleanup" EXIT
