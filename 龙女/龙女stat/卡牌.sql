INSERT INTO item_pay_user_rec_times_card_probability_up_star_stat(`pool_type`,`date`,`period`,`orders`,`reprint`,`uid`,`cardid`,`poolid`,`times`,`pay_dau`,`totalTimes`,`totalMoney`,`cardstar`,`starType`)
SELECT 'ur14' pooltype,
              recResult.*,
              ifnull(cardstar,-1) cardstar,
              ifnull(starType,1) starType
FROM
  (SELECT '2023-12-23' date,recPeriod.period,
                            recPeriod.orders,
                            recPeriod.reprint,
                            recur14.*,
                            totalPayUids,
                            recur14Total.totalTimes,
                            pay.money
   FROM
     (SELECT uid,
             cardid,
             poolid,
             af_times times
      FROM stat2.user_ur_recruit_stat
      WHERE date='2023-12-23'
        AND cardid!=''
        AND substring(cardid,2,1)<=4
        AND af_times>0) recur14,
     (SELECT uid,
             sum(money) money
      FROM stat4.pay_syn_day
      WHERE date='2023-12-23'
        AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136)
      GROUP BY uid) pay ,
     (SELECT uid,
             sum(af_times) totalTimes
      FROM stat2.user_ur_recruit_stat
      WHERE date='2023-12-23'
        AND cardid!=''
        AND af_times>0
        AND substring(cardid,2,1)<=4
      GROUP BY uid) recur14Total,
     (SELECT cardid,
             ur_poolid poolid,
             datediff('2023-12-23',startdate)+1 period,
             orders,
             ur_reprint reprint
      FROM stat2.card_probability_up_cycle
      WHERE substring(cardid,2,1)<=4
        AND zoneid=1
        AND ur_poolid!=0
        AND startdate<='2023-12-23'
        AND enddate>='2023-12-23') recPeriod,
     (SELECT count(DISTINCT uid) totalPayUids
      FROM stat4.pay_syn_day
      WHERE date='2023-12-23'
        AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136)) paySummary
   WHERE recur14.uid = pay.uid
     AND recur14.uid = recur14Total.uid
     AND recPeriod.cardid = recur14.cardid
     AND recPeriod.poolid = recur14.poolid) recResult
LEFT JOIN
  (SELECT uid,
          cardid,
          ifnull(starType,-1) starType,
          ifnull(cardstar,-1) cardstar
   FROM
     (SELECT itemPayRed.uid,
             itemPayRed.cardid,
             ifnull(itemPayRed.starType,uidPayGold.starType) starType,
             ifnull(itemPayRed.cardstar,uidPayGold.starGold) cardstar
      FROM
        (SELECT itemPay.uid,
                itemPay.cardid,
                cardstar,
                starType
         FROM
           (SELECT pay.uid,
                   card.cardid
            FROM
              (SELECT DISTINCT uid
               FROM stat4.pay_syn_day
               WHERE date='2023-12-23'
                 AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136)) pay,
              (SELECT cardid
               FROM stat2.card_probability_up_cycle
               WHERE substring(cardid,2,1)<=4
                 AND zoneid=1
                 AND ur_poolid!=0
                 AND startdate<='2023-12-23'
                 AND enddate>='2023-12-23')card) itemPay
         LEFT JOIN
           (SELECT uid,
                   cardid,
                   starType,
                   max(cardstar) cardstar
            FROM
              (SELECT uid,
                      cardid,
                      0 cardstar,
                        1 starType
               FROM stat2.user_card_stat
               WHERE date<='2023-12-23'
                 AND uid IN
                   (SELECT DISTINCT uid
                    FROM stat4.pay_syn_day
                    WHERE date='2023-12-23'
                      AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136))
                 AND cardid IN
                   (SELECT cardid
                    FROM stat2.card_probability_up_cycle
                    WHERE substring(cardid,2,1)<=4
                      AND zoneid=1
                      AND ur_poolid!=0
                      AND startdate<='2023-12-23'
                      AND enddate>='2023-12-23')
                 AND cardid IN
                   (SELECT cardid
                    FROM stat2.card_information
                    WHERE is_original_ul!=0)
               UNION SELECT uid,
                            cardid,
                            0 cardstar,
                              1 starType
               FROM stat2.user_card_awaken_active_stat
               WHERE date<='2023-12-23'
                 AND uid IN
                   (SELECT DISTINCT uid
                    FROM stat4.pay_syn_day
                    WHERE date='2023-12-23'
                      AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136))
                 AND cardid IN
                   (SELECT cardid
                    FROM stat2.card_probability_up_cycle
                    WHERE substring(cardid,2,1)<=4
                      AND zoneid=1
                      AND ur_poolid!=0
                      AND startdate<='2023-12-23'
                      AND enddate>='2023-12-23')
                 AND cardid IN
                   (SELECT cardid
                    FROM stat2.card_information
                    WHERE is_original_ul!=0)
               UNION SELECT uid,
                            cardid,
                            max(cardstar) cardstar,
                            1 starType
               FROM stat2.user_card_awaken_star_level_stat
               WHERE date<='2023-12-23'
                 AND uid IN
                   (SELECT DISTINCT uid
                    FROM stat4.pay_syn_day
                    WHERE date='2023-12-23'
                      AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136))
                 AND cardid IN
                   (SELECT cardid
                    FROM stat2.card_probability_up_cycle
                    WHERE substring(cardid,2,1)<=4
                      AND zoneid=1
                      AND ur_poolid!=0
                      AND startdate<='2023-12-23'
                      AND enddate>='2023-12-23')
               GROUP BY uid,
                        cardid) a
            GROUP BY uid,
                     cardid,
                     starType) urRed ON itemPay.uid = urRed.uid
         AND itemPay.cardid = urRed.cardid) itemPayRed
      LEFT JOIN
        (SELECT uid,
                cardid,
                starType,
                max(cardstar) starGold
         FROM
           (SELECT 0 starType,
                     uid,
                     cardid,
                     0 cardstar
            FROM user_card_ur_gold_stat gold
            WHERE date<='2023-12-23'
              AND uid IN
                (SELECT DISTINCT uid
                 FROM stat4.pay_syn_day
                 WHERE date='2023-12-23'
                   AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136))
              AND cardid IN
                (SELECT cardid
                 FROM stat2.card_probability_up_cycle
                 WHERE substring(cardid,2,1)<=4
                   AND zoneid=1
                   AND ur_poolid!=0
                   AND startdate<='2023-12-23'
                   AND enddate>='2023-12-23')
              AND cardid IN
                (SELECT cardid
                 FROM stat2.card_information
                 WHERE is_original_ul!=0)
            UNION SELECT 0 starType,
                           uid,
                           cardid,
                           cardstar
            FROM user_card_ur_star_gold_level_stat gold
            WHERE date<='2023-12-23'
              AND uid IN
                (SELECT DISTINCT uid
                 FROM stat4.pay_syn_day
                 WHERE date='2023-12-23'
                   AND itemid IN (92037,92038,92039,92040,92041,92070,92071,92073,92074,92075,93001,93002,93003,93004,93005,93006,93007,93008,93009,93010,93011,93012,93013,93014,93015,93016,93017,93018,93019,93020,93021,93022,93023,93024,93025,93026,93027,93028,93029,93030,93031,93032,93033,93034,93035,93036,93037,93038,93039,93040,93041,93042,93043,93044,93045,93046,93047,93048,93049,96001,96002,96004,96005,96006,96022,96023,96025,96026,96027,96029,96030,96032,96033,96034,96060,96061,96063,96064,96065,96081,96082,96084,96085,96086,96089,96090,96092,96093,96094,96116,96117,96119,96120,96121,96124,96125,96127,96128,96129,96131,96132,96134,96135,96136))
              AND cardid IN
                (SELECT cardid
                 FROM stat2.card_probability_up_cycle
                 WHERE substring(cardid,2,1)<=4
                   AND zoneid=1
                   AND ur_poolid!=0
                   AND startdate<='2023-12-23'
                   AND enddate>='2023-12-23')
              AND cardid IN
                (SELECT cardid
                 FROM stat2.card_information
                 WHERE is_original_ul!=0))uidCardStarGold
         GROUP BY uid,
                  cardid,
                  starType) uidPayGold ON itemPayRed.uid = uidPayGold.uid
      AND itemPayRed.cardid = uidPayGold.cardid
      AND itemPayRed.cardstar IS NULL) urcardstar) recCycleCardStar 
ON recResult.uid = recCycleCardStar.uid
AND recResult.cardid = recCycleCardStar.cardid