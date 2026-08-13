SELECT order_id, sku, COUNT(*) AS reservation_count
FROM inventory_reservations
GROUP BY order_id, sku
HAVING COUNT(*) > 1
ORDER BY order_id, sku;
