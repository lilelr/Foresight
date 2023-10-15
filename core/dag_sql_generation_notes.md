## hash Aggregate

1. nodeID(33) HashAggregate(keys=[d_week_seq#99], functions=[partial_sum(CASE WHEN (d_day_name#109 = Sunday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#109 = Monday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#109 = Tuesday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#109 = Wednesday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#109 = Thursday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#109 = Friday) THEN sales_price#17 END), partial_sum(CASE WHEN (d_day_name#109 = Saturday) THEN sales_price#17 END)])

2. nodeID(26) HashAggregate(keys=[ctr_store_sk#2], functions=[avg(ctr_total_return#3)])

3. nodeID(13) HashAggregate(keys=[sr_customer_sk#7, sr_store_sk#11], functions=[sum(sr_return_amt#15)])

4. nodeID(15) HashAggregate(keys=[sr_customer_sk#7, sr_store_sk#11], functions=[partial_sum(sr_return_amt#15)])

5. nodeID(76) HashAggregate(keys=[c_customer_id#2068, c_first_name#2075, c_last_name#2076, c_preferred_cust_flag#2077, c_birth_country#2081, c_login#2082, c_email_address#2083, d_year#2125], functions=[partial_sum(((((cs_ext_list_price#2110 - cs_ext_wholesale_cost#2109) - cs_ext_discount_amt#2107) + cs_ext_sales_price#2108) / 2.0))])

6. nodeID(74) HashAggregate(keys=[c_customer_id#2068, c_first_name#2075, c_last_name#2076, c_preferred_cust_flag#2077, c_birth_country#2081, c_login#2082, c_email_address#2083, d_year#2125], functions=[sum(((((cs_ext_list_price#2110 - cs_ext_wholesale_cost#2109) - cs_ext_discount_amt#2107) + cs_ext_sales_price#2108) / 2.0))])

7. nodeID(32) HashAggregate(keys=[d_month_seq#119], functions=[])

8. nodeID(4) HashAggregate(keys=[i_item_id#65], functions=[partial_avg(ss_quantity#14), partial_avg(ss_list_price#16), partial_avg(ss_coupon_amt#23), partial_avg(ss_sales_price#17)])


### errors

expression 'tb1.sr_return_amt' is neither present in the group by, nor is it an aggregate function.

SELECT tb1.sr_customer_sk, tb1.sr_store_sk, tb1.sr_return_amt, sum(sr_return_amt )
 改为 SELECT tb1.sr_customer_sk, tb1.sr_store_sk,  sum(tb1.sr_return_amt )
 
 
 ### Filter 
Filter ((isnotnull(d_moy#11) AND (d_moy#11 = 11)) AND isnotnull(d_date_sk#3))
 
 Filter isnotnull(d_moy) AND (d_moy= 11)) AND isnotnull(d_date_sk))
 
 Filter _(((p_channel_email#95 = N) OR (p_channel_event#100 = N)) AND isnotnull(p_promo_sk#86))_
 
Filter ((isnotnull(sr_returned_date_sk#4) AND isnotnull(sr_store_sk#11)) AND isnotnull(sr_customer_sk#7))
 
 Filter isnotnull(ws_sold_date_sk#27)
 
Filter ((isnotnull(i_manufact_id#67) AND (i_manufact_id#67 = 128)) AND isnotnull(i_item_sk#54))
 
 
 nodeID(28) Filter (substr(ca_zip#94, 1, 5) INSET 10144, 10336, 10390, 10445, 10516, 10567, 11101, 11356, 11376, 11489, 11634, 11928, 12305, 13354, 13375, 13376, 1
 
 nodeID(23) Filter (((isnotnull(d_date#105) AND (cast(d_date#105 as date) >= 2000-08-23)) AND (cast(d_date#105 as date) <= 2000-09-06)) AND isnotnull(d_date_sk#103))
 
 Q41
 Filter (item_cnt#0L > 0) 
 Q44
 Filter (isnotnull(rank_col#1) AND (rank_col#1 > (0.9 * Subquery subquery#3, [id=#119])))
'rank_col'

 #### Filter handling methods
 1  delete the "#id", then split by 'AND' , return an array of items
    import re
    line = re.sub(r"""#\d+""", " ", line)

 2  For each item, judge whether isnotnull
    isnotnull(d_moy),  (d_moy= 11)), (((p_channel_email = N) OR (p_channel_event = N))
 2.1 if isnotnull exists, select the "()"
 2.2 if ' = ' exists, parenthesis; multiple '=' exists, select the first one
 2.3 other items save into the special_str field