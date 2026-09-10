package io.gtfs;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryTest {
  KeyedOneInputStreamOperatorTestHarness<String,String,String> harness() throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new LakehouseJob.Deduplicate(120000)),
        value -> LakehouseJob.parse(value).path("event_id").asText(), Types.STRING);
  }
  @Test void restoredOperatorRejectsUnseenOldEventsBeforeNewWatermarksArrive() throws Exception {
    var before = harness(); before.open();
    before.processElement(new StreamRecord<>("{\"event_id\":\"a\",\"observed_at\":1000000}", 1000000));
    before.processWatermark(new Watermark(1500000));
    var snapshot = before.snapshot(1, 1); before.close();
    var after = harness(); after.initializeState(snapshot); after.open();
    after.processElement(new StreamRecord<>("{\"event_id\":\"b\",\"observed_at\":1000000}", 1000000));
    assertTrue(after.extractOutputStreamRecords().isEmpty());
    assertEquals(1, after.getSideOutput(LakehouseJob.LATE).size());
    after.processElement(new StreamRecord<>("{\"event_id\":\"c\",\"observed_at\":1600000}", 1600000));
    assertEquals(1, after.extractOutputStreamRecords().size());
    after.close();
  }
  @Test void restoredWindowDoesNotReopenForPreviouslyBufferedEvents() throws Exception {
    String event = new RouteMetricsTest().event("a", 1000000, 60);
    var before = new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new RouteMetrics()), RouteMetrics::key, Types.STRING);
    before.open(); before.processElement(new StreamRecord<>(event, 1000000));
    before.processWatermark(new Watermark(1500000));
    assertEquals(1, before.extractOutputStreamRecords().size());
    var snapshot = before.snapshot(1, 1); before.close();
    var after = new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new RouteMetrics()), RouteMetrics::key, Types.STRING);
    after.initializeState(snapshot); after.open();
    after.processElement(new StreamRecord<>(new RouteMetricsTest().event("b", 1000001, 120), 1000001));
    assertTrue(after.extractOutputStreamRecords().isEmpty());
    assertEquals(1, after.getSideOutput(LakehouseJob.LATE).size());
    assertNull(after.getSideOutput(RouteMetrics.INPUTS));
    after.close();
  }
}
