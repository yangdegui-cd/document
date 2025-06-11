
SELECT *
FROM stat2.card_probability_up_cycle
group by orders
ORDER BY startdate  



delete FROM stat2.card_probability_up_cycle where orders = 343;


SELECT * from stat2.card_probability_up_wish_pool where activity_id=959;

