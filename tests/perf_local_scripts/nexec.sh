#!/bin/bash
# cat ip.txt | xargs -t -I % ./ssh.sh % "$1"
cat ip.txt | xargs -t -P 10 -I % ./ssh.sh % "$1"
