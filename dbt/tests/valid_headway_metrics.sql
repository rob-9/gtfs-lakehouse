SELECT * FROM {{ ref('route_reliability') }}
WHERE arrival_count < 0 OR headway_count < 0 OR headway_count > arrival_count
   OR mean_headway_seconds < 0
   OR (headway_count = 0 AND mean_headway_seconds IS NOT NULL)
   OR (headway_count > 0 AND mean_headway_seconds IS NULL)
   OR (generation = 'live-v3' AND mean_headway_seconds > 7200)
