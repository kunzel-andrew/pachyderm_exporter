#!/bin/bash

if [ -n "$(gofmt -l $(find * -name *.go | grep -v vendor))" ]; then
    echo "Go code is not formatted:"
    gofmt -d $(find * -name '*.go' | grep -v 'vendor')
    exit 1
fi
