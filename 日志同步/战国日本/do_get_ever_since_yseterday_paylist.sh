
#!/bin/bash
for i in $(seq 1 $1)
do 
        sh ./do_get_pay.sh $i
done

vim  do_get_ever_since_yseterday_paylist.sh

chmod +x do_get_ever_since_yseterday_paylist.sh

./do_get_ever_since_yseterday_paylist.sh 45