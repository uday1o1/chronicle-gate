SELECT order_id, sku, quantity, observed_at
FROM inventory_reservations
ORDER BY order_id, sku;
