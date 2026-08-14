SELECT order_id, fulfillment_mode
FROM fulfillment_projection
WHERE fulfillment_mode NOT IN ('standard', 'expedited')
ORDER BY order_id
