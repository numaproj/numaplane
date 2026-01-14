#!/bin/bash
set -e

# Start the main process in the background
/manager --health-probe-bind-address=:8081 --metrics-bind-address=:8080 --leader-elect &
export NUMAPLANE_PID=$!
## Write NUMAPLANE_PID to a file
echo $NUMAPLANE_PID > /numaplane.pid

# Function to handle signals
# TODO: PID needs to be changed to NUMAPLANE_PID
trap "echo 'Stopping PID $PID'; kill $PID; wait $PID; /manager --health-probe-bind-address=:8081 --metrics-bind-address=:8080 --leader-elect &" SIGTERM SIGINT

# Wait indefinitely
while true
do
  tail -f /dev/null & wait ${!}
done