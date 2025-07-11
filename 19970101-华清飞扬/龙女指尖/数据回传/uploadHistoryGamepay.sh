#!/bin/bash
for i in $(seq 1 $1)
do 
        sh ./uploadGamepay.sh $i
done

