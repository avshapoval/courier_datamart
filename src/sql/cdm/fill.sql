TRUNCATE TABLE cdm.dm_courier_ledger;
WITH preprocessed AS (
	SELECT 
	dc.id,
	max(dc."name") "name",
	extract(YEAR FROM fod.delivery_ts) settl_year,
	extract(MONTH FROM fod.delivery_ts) settl_month,
	count(do2.id) ord_count,
	sum(do2."sum") ord_total,
	avg(fod.rating) rate_avg,
	sum(do2."sum") * 0.25 ord_pr_fee,
	sum(fod.tip_sum) tip_sum
	FROM dds.fct_order_deliveries fod
		LEFT JOIN dds.dm_couriers dc ON fod.courier_id = dc.id 
		LEFT JOIN dds.dm_restaurants dr ON fod.restaurant_id = dr.id
		LEFT JOIN dds.dm_orders do2 ON do2.id = fod.order_id
	GROUP BY dc.id, extract(YEAR FROM fod.delivery_ts), extract(MONTH FROM fod.delivery_ts)
),
courier_order_sum AS (
	SELECT id,
	settl_year , 
	settl_month,
	CASE 
		WHEN rate_avg >= 4.9 THEN (SELECT MAX(order_sum) FROM (VALUES(0.1 * ord_total, (200 * ord_count)::numeric)) AS t(order_sum))
		WHEN rate_avg < 4.9 AND rate_avg >= 4.5 THEN (SELECT MAX(order_sum) FROM (VALUES(0.08 * ord_total, (175 * ord_count)::numeric)) AS t(order_sum))
		WHEN rate_avg < 4.5 AND rate_avg >= 4 THEN (SELECT MAX(order_sum) FROM (VALUES(0.07 * ord_total, (150 * ord_count)::numeric)) AS t(order_sum))
		WHEN rate_avg < 4 THEN (SELECT MAX(order_sum) FROM (VALUES(0.05 * ord_total, (100 * ord_count)::numeric)) AS t(order_sum))
	END AS coorsum
	FROM preprocessed
)
INSERT INTO cdm.dm_courier_ledger
SELECT
	c.id,
	"name",
	p.settl_year,
	p.settl_month,
	ord_count,
	ord_total,
	rate_avg,
	ord_pr_fee,
	c.coorsum "courier_order_sum",
	tip_sum,
	c.coorsum + 0.95 * tip_sum "courier_reward_sum"
FROM preprocessed p  INNER JOIN courier_order_sum c ON p.id = c.id AND p.settl_year = c.settl_year AND p.settl_month = c.settl_month


