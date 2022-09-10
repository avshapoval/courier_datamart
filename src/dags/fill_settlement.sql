INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
SELECT 
	dr.id,
	max(dr.restaurant_name) rname,
	dt."date",
	sum(fps."count") oc,
	sum(fps.total_sum) ots,
	sum(fps.bonus_payment) obps,
	sum(fps.bonus_grant) bg,
	sum(fps.total_sum)*0.25 opf,
	sum(fps.total_sum) - sum(fps.bonus_payment) - sum(fps.total_sum)*0.25 rrs
FROM dds.dm_orders do2
	INNER JOIN dds.dm_restaurants dr ON do2.restaurant_id = dr.id
	INNER JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id 
	INNER JOIN dds.fct_product_sales fps ON fps.order_id = do2.id
WHERE do2.order_status = 'CLOSED' AND dt."date" >= (SELECT MAX(settlement_date) FROM cdm.dm_settlement_report)
GROUP BY dr.id, dt."date"
ON CONFLICT ON CONSTRAINT dm_settlement_report_unique DO UPDATE
SET orders_count = EXCLUDED.orders_count,
	orders_total_sum = excluded.orders_total_sum,
	orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
	orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
	order_processing_fee = excluded.order_processing_fee,
	restaurant_reward_sum = excluded.restaurant_reward_sum