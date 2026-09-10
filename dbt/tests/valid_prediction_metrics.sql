SELECT * FROM {{ ref('route_reliability') }}
WHERE prediction_count < 0 OR canceled_trip_count < 0
   OR adherence_rate < 0 OR adherence_rate > 1
   OR (prediction_count = 0 AND mean_delay_seconds IS NOT NULL)
