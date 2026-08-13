SELECT event_id, count(*) AS processed_count
FROM processed_events
GROUP BY event_id
HAVING count(*) > 1
ORDER BY event_id;
