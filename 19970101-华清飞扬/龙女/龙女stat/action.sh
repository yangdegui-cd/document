cd /data/stat/syn2jishubu
vim do_get_gamepay.sh
chmod +x do_get_gamepay.sh

15 0 * * * sh /data/stat/syn2jishubu/do_get_gamepay.sh 1 >> /data/stat/syn2jishubu/do_get_gamepay.log 2>&1 &

vim do_get_coinpay.sh
chmod +x do_get_coinpay.sh


17 0 * * * sh /data/stat/syn2jishubu/do_get_coinpay.sh 1 >> /data/stat/syn2jishubu/do_get_coinpay.log 2>&1 &