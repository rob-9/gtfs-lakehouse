SELECT generation, agency_id, route_id, direction_id, service_date, window_start
FROM {{ ref('route_reliability') }}
GROUP BY generation, agency_id, route_id, direction_id, service_date, window_start
HAVING count() > 1
