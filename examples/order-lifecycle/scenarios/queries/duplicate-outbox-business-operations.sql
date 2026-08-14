SELECT business_key AS order_id,
       'transactional-outbox'::text AS sku,
       count(*) AS reservation_count
FROM outbox
GROUP BY business_key
HAVING count(*) <> 1
ORDER BY business_key;
