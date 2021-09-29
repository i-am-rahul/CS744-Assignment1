#!/bin/bash
trap "exit" INT

parallel-ssh -i -h /users/agabhin/followers -O StrictHostKeyChecking=no hostname
ssh -tt agabhin@node1.agabhin-106662.uwmadison744-f21-pg0.wisc.cloudlab.us << EOF 
/users/agabhin/collect_updated_stats.sh $1 $2 &
exit
EOF
ssh -tt agabhin@node2.agabhin-106662.uwmadison744-f21-pg0.wisc.cloudlab.us <<EOF 
/users/agabhin/collect_updated_stats.sh $1 $2 & 
exit
EOF
/users/agabhin/collect_updated_stats.sh $1 $2 &
