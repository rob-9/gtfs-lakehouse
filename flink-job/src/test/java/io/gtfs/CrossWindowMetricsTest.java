package io.gtfs;

import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CrossWindowMetricsTest {
  static String visit(String id, String trip, long timestamp) {
    return """
      {"event_id":"%s","observed_at":%d,"agency_id":"demo","route_id":"R1",
      "direction_id":0,"service_date":"2026-09-10","trip_id":"%s","vehicle_id":"V1",
      "payload":{"trip":{},"trip_update":null,"vehicle_position":{"current_status":"STOPPED_AT","stop_id":"S1"}}}
      """.formatted(id, timestamp, trip);
  }
  static KeyedOneInputStreamOperatorTestHarness<String,String,String> harness() throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new CrossWindowMetrics()), CrossWindowMetrics::key, Types.STRING);
  }
  @Test void sharedGoldenWindowsMatchIndependentOracleContract() throws Exception {
    var fixture = LakehouseJob.JSON.readTree(java.nio.file.Files.readString(
        java.nio.file.Path.of(System.getProperty("basedir"), "..", "tests", "fixtures", "cross_window_headways.json")));
    var h = harness(); h.open();
    var events = new ArrayList<String>(); fixture.path("events").forEach(event -> events.add(event.toString()));
    Collections.reverse(events);
    for (String event : events) h.processElement(new StreamRecord<>(event));
    h.processWatermark(new Watermark(1500000));
    var rows = h.extractOutputStreamRecords();
    assertEquals(fixture.path("expected").size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      var actual = LakehouseJob.parse(rows.get(i).getValue());
      var fields = fixture.path("expected").get(i).fields();
      while (fields.hasNext()) {
        var entry = fields.next();
        if (entry.getValue().isNumber()) assertEquals(entry.getValue().asDouble(), actual.path(entry.getKey()).asDouble(), entry.getKey());
        else assertEquals(entry.getValue(), actual.path(entry.getKey()), entry.getKey());
      }
    }
    h.close();
  }
  @Test void outOfOrderWindowsBridgeAndDwellDoesNotAddAnArrival() throws Exception {
    var h = harness(); h.open();
    for (String value : List.of(visit("b", "T2", 310000), visit("dwell", "T1", 305000), visit("a", "T1", 290000)))
      h.processElement(new StreamRecord<>(value));
    h.processWatermark(new Watermark(720000));
    var rows = h.extractOutputStreamRecords();
    assertEquals(2, rows.size());
    var second = LakehouseJob.parse(rows.get(1).getValue());
    assertEquals(1, second.path("arrival_count").asInt());
    assertEquals(1, second.path("headway_count").asInt());
    assertEquals(20.0, second.path("mean_headway_seconds").asDouble());
    assertEquals(1, second.path("context_event_count").asInt());
    assertEquals("live-v3", second.path("generation").asText());
    h.close();
  }
  @Test void checkpointRestoresPredecessorAndAdmissionCutoff() throws Exception {
    var before = harness(); before.open();
    before.processElement(new StreamRecord<>(visit("a", "T1", 290000)));
    before.processWatermark(new Watermark(420000));
    var snapshot = before.snapshot(1, 1); before.close();
    var after = harness(); after.initializeState(snapshot); after.open();
    after.processElement(new StreamRecord<>(visit("old", "T3", 280000)));
    assertEquals(1, after.getSideOutput(LakehouseJob.LATE).size());
    after.processElement(new StreamRecord<>(visit("b", "T2", 310000)));
    after.processWatermark(new Watermark(720000));
    assertEquals(20.0, LakehouseJob.parse(after.extractOutputStreamRecords().get(0).getValue()).path("mean_headway_seconds").asDouble());
    after.processWatermark(new Watermark(10000000));
    assertEquals(0, after.numKeyedStateEntries());
    after.close();
  }
  @Test void expiredAndDifferentStopPredecessorsDoNotProduceHeadways() throws Exception {
    var history = new ArrayList<String>();
    history.add(visit("a", "T1", 0));
    var metric = CrossWindowMetrics.aggregate(List.of(visit("b", "T2", CrossWindowMetrics.LOOKBACK + 1)), history);
    assertTrue(metric.path("mean_headway_seconds").isNull());
    var isolated = CrossWindowMetrics.aggregate(List.of(visit("c", "T3", CrossWindowMetrics.LOOKBACK + 2).replace("S1", "S2")), history);
    assertEquals(0, isolated.path("headway_count").asInt());
  }
}
