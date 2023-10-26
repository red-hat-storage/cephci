#!/bin/bash

# Check for the required parameter
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <IP_ADDRESS> <SLEEP_DURATION_SECONDS>"
    exit 1
fi

# Extract the IP address and sleep duration from the command-line parameters
target_ip="$1"
sleep_duration="$2"

# Get the network interface associated with the target IP
network_interface=$(netstat -ie | grep -B1 "$target_ip" | head -n1 | awk '{print $1}' | sed 's/://')


if [ -z "$network_interface" ]; then
    echo "Network interface not found for IP address $target_ip."
    exit 1
fi

# Add rules to block traffic on the specified interface
echo "Blocking traffic on interface $network_interface for $sleep_duration seconds..."
sudo iptables -A INPUT -i $network_interface -j DROP
sudo iptables -A OUTPUT -o $network_interface -j DROP

# Wait for the specified duration
sleep "$sleep_duration"

# Remove the rules to allow traffic again
echo "Restoring traffic on interface $network_interface..."
sudo iptables -D INPUT -i $network_interface -j DROP
sudo iptables -D OUTPUT -o $network_interface -j DROP

echo "Network interruption complete."

# Exit the script
exit 0
