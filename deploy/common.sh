#!/bin/bash

function waitPort() {
  for _ in `seq 1 20`; do
    echo -n .
    if nc -z localhost $1; then
        break
    fi
    sleep 0.5
  done
}