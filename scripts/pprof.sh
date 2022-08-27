#!/usr/bin/env bash

#
# This script generates memory and cpu profiles.
#

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")

$("${SCRIPT_PATH}/killweb.sh")

profilesFor=(bigcsvreader gocsvreadall gocsvreadonebyone)
port=8084
for profileFor in "${profilesFor[@]}"
do
	  echo "Handling profiles for ${profileFor}"
    go run "${SCRIPT_PATH}/../cmd/pprof/main.go" -for="${profileFor}"
    go tool pprof -http=":${port}" "mem_${profileFor}.prof" &
    port=$(( port + 1 ))
    go tool pprof -http=":${port}" "cpu_${profileFor}.prof" &
    port=$(( port + 1 ))
done
