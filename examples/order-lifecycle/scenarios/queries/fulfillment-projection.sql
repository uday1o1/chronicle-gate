SELECT order_id, event_id, fulfillment_mode, status, updated_at
FROM fulfillment_projection
ORDER BY order_id
