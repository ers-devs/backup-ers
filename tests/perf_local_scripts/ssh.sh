#!/bin/bash
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/.ssh/xo_rsa root@$1 $2
