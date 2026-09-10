package io.gtfs;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RouteMetricsTest {
  String event(String id, long timestamp, int delay) {
    return """
      {"event_id":"%s","observed_at":%d,"agency_id":"demo","route_id":"R1",
       "direction_id":0,"service_date":"2026-09-10","trip_id":"T1","vehicle_id":"V1",
       "payload":{"trip":{"schedule_relationship":"SCHEDULED"},"vehicle_position":null,
       "trip_update":{"stop_time_updates":[{"stop_id":"S1","stop_sequence":1,"arrival_delay":%d}]}}}
      """.formatted(id, timestamp, delay);
  }
  @Test void predictionCorrectionsReplaceSamplesAndRetriesDoNotInflateCounts() throws Exception {
    String before = event("a", 1789052400000L, 60), after = event("b", 1789052401000L, 120);
    var metric = RouteMetrics.aggregate(List.of(after, before, after));
    assertEquals(1, metric.path("prediction_count").asInt());
    assertEquals(120, metric.path("mean_delay_seconds").asDouble());
    assertEquals(1, metric.path("vehicle_count").asInt());
    assertEquals(RouteMetrics.aggregate(List.of(before, after)), metric);
  }
  @Test void quantileUsesNearestRankAndNoHeadwayIsInvented() throws Exception {
    var metric = RouteMetrics.aggregate(List.of(event("a", 1789052400000L, 60)));
    assertEquals(60, metric.path("p95_delay_seconds").asInt());
    assertTrue(metric.path("mean_headway_seconds").isNull());
    assertEquals(0, metric.path("arrival_count").asInt());
  }
  @Test void slashCharactersCannotCollideInAggregateKeys() {
    String value = event("a", 1789052400000L, 60);
    String left = value.replace("demo", "a/b").replace("R1", "c");
    String right = value.replace("demo", "a").replace("R1", "b/c");
    assertNotEquals(RouteMetrics.key(left), RouteMetrics.key(right));
  }
  @Test void vehicleObservationDoesNotClearExplicitCancellation() throws Exception {
    String canceled = event("a", 1789052400000L, 60).replace("SCHEDULED", "CANCELED");
    var vehicle = LakehouseJob.parse(event("b", 1789052401000L, 60));
    ((com.fasterxml.jackson.databind.node.ObjectNode) vehicle.path("payload")).putNull("trip_update");
    var metric = RouteMetrics.aggregate(List.of(canceled, vehicle.toString()));
    assertEquals(1, metric.path("canceled_trip_count").asInt());
    assertEquals(0, metric.path("prediction_count").asInt());
  }
}
