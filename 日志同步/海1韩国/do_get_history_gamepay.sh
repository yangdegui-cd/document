#!/bin/bash
for i in $(seq 1 $1)
do 
        sh ./do_get_gamepay.sh $i
done

