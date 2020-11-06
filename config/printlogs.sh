#!/bin/bash


echo "Print errlogs"
cd cass/log/
for ((i=1; i<=$1; i++)); do
    VAR=$(tail -1 $i.err.log)
    [[ "$VAR" == "----------------------------------------------------" ]] && echo "Job ${i} done" || echo "Job ${i} not done"
done
echo "Complete printing"