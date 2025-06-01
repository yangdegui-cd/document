day=`date -d "1 days ago" "+%Y %m %d %H"`
day2=`date -d "1 days ago" "+%Y%m%d"`
/data/bigdata/data/do_ndpay_get.sh s1_szhqrb ${day}
/data/bigdata/data/do_ndpay_get.sh s2_szhqrb ${day}
/data/bigdata/data/do_ndpay_get.sh s3_szhqrb ${day}
/data/bigdata/data/do_ndpay_get.sh s4_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s5_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s6_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s7_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s8_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s9_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s10_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s11_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s12_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s13_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s14_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s15_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s16_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s17_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s18_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s19_szhqrb ${day}
/data/bigdata/data/do_dpay_get.sh s20_szhqrb ${day}
/data/bigdata/data/upload_to_bigdata.sh ${day2}