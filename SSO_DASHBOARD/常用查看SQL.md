##查看卡池

```
SELECT *
FROM stat2.card_probability_up_cycle
group by orders,startdate
ORDER BY startdate DESC 
LIMIT 10;
```

##查看某个卡池

```
SELECT *
FROM stat2.card_probability_up_cycle
WHERE cardid = 5312
group by orders,startdate
ORDER BY startdate DESC 
LIMIT 10;

```