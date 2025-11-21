#!/usr/bin/env bash

printf "Stopping benchmark processes (opa-bench, opb-bench)...\n"
pkill -f "bin/opa.*-group-id opa-bench" >/dev/null 2>&1 || true
pkill -f "bin/opb.*-group-id opb-bench" >/dev/null 2>&1 || true
sleep 1
printf "Cleanup complete.\n"
