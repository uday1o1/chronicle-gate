WITH projected AS (
  SELECT order_id, event_id, fulfillment_mode, status, updated_at
  FROM fulfillment_projection
)
SELECT order_id, event_id, fulfillment_mode, status, updated_at
FROM projected
ORDER BY order_id
