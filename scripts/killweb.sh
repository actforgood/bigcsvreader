#!/usr/bin/env bash

#
# This script kills the http server started in background with pprof.sh
#

for port in 8084 8085 8086 8087 8088 8089; do \
    ps aux | grep "go tool pprof -http=:${port}" | grep -v "grep go tool pprof -http=:${port}" | awk '{print $2}' | xargs kill
done
