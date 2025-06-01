select 'payment',time,uid,openid,channel,region,level,viplevel,devicetype,paytime,paychannel,payway,billno,orderid,status,0,itemid,currency,money,amount,diamond,giftconsume,null,id,null,device,origin from pay where status = 0
union all
select 'payment',time,uid,openid,channel,region,level,viplevel,null,null,paychannel,null,billno,null,status,1,itemid,currency,money,amount,null,null,null,id,null,null,null from coin_pay where status = 0

mysql -h10.10.10.4 -udragon -pdragongirl