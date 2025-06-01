mysql -uroot -proot -N  -e "SELECT * FROM stat2.card_probability_up_cycle group by orders ORDER BY startdate  " -B -s | sed 's/\t/,/g' > ./card_probability_up_cycle.sql

mysql -uroot -proot -N  -e "SELECT * FROM stat2.card_probability_up_wish_pool group by orders ORDER BY startdate  " -B -s | sed 's/\t/,/g' > ./card_probability_up_wish_pool.sql
