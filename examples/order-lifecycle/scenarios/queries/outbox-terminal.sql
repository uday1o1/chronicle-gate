SELECT o.order_id,
       o.status,
       x.event_id,
       x.publish_attempts,
       (x.published_at IS NOT NULL) AS published
FROM orders o
JOIN outbox x ON x.aggregate_id = o.order_id
ORDER BY o.order_id;
