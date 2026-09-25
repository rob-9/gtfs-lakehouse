package io.gtfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Final route windows with a bounded, checkpointed stop-visit history. */
public class CrossWindowMetrics extends KeyedProcessFunction<String,String,String>
    implements org.apache.flink.streaming.api.checkpoint.CheckpointedFunction {
  static final long LOOKBACK = 7200000;
  static final long LATENESS = 120000;
  private transient ListState<String> pending;
  private transient ListState<String> visits;
  private transient ListState<Long> cutoffState;
  private long cutoff = Long.MIN_VALUE;

  static String key(String value) {
    var key = (com.fasterxml.jackson.databind.node.ArrayNode) LakehouseJob.parse(RouteMetrics.key(value));
    key.remove(4);
    return key.toString();
  }
  @Override public void open(Configuration config) {
    pending = getRuntimeContext().getListState(new ListStateDescriptor<>("cross-window-pending", String.class));
    visits = getRuntimeContext().getListState(new ListStateDescriptor<>("cross-window-visits", String.class));
  }
  @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
    JsonNode event = LakehouseJob.parse(value);
    long end = Math.floorDiv(event.path("observed_at").asLong(), RouteMetrics.WINDOW) * RouteMetrics.WINDOW + RouteMetrics.WINDOW + LATENESS;
    cutoff = Math.max(cutoff, ctx.timerService().currentWatermark());
    if (end <= cutoff) {
      ObjectNode rejected = event.deepCopy(); rejected.put("generation", "live-v3");
      ctx.output(LakehouseJob.LATE, rejected.toString()); return;
    }
    pending.add(value);
    ObjectNode admitted = event.deepCopy(); admitted.put("generation", "live-v3");
    ctx.output(RouteMetrics.INPUTS, admitted.toString());
    ctx.timerService().registerEventTimeTimer(end);
  }
  @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    cutoff = Math.max(cutoff, ctx.timerService().currentWatermark());
    var windows = new TreeMap<Long,List<String>>();
    var remaining = new ArrayList<String>();
    for (String value : pending.get()) {
      long start = Math.floorDiv(LakehouseJob.parse(value).path("observed_at").asLong(), RouteMetrics.WINDOW) * RouteMetrics.WINDOW;
      if (start + RouteMetrics.WINDOW + LATENESS <= timestamp)
        windows.computeIfAbsent(start, ignored -> new ArrayList<>()).add(value);
      else remaining.add(value);
    }
    var history = new ArrayList<String>(); visits.get().forEach(history::add);
    for (var entry : windows.entrySet()) {
      long start = entry.getKey();
      history.removeIf(value -> LakehouseJob.parse(value).path("observed_at").asLong() < start - LOOKBACK);
      out.collect(aggregate(entry.getValue(), history).toString());
    }
    // Event-time cleanup also runs when the route receives no further records.
    history.removeIf(value -> LakehouseJob.parse(value).path("observed_at").asLong() + LOOKBACK + RouteMetrics.WINDOW + LATENESS < timestamp);
    visits.update(history); pending.update(remaining);
    if (!history.isEmpty()) {
      long latest = history.stream().mapToLong(value -> LakehouseJob.parse(value).path("observed_at").asLong()).max().orElseThrow();
      ctx.timerService().registerEventTimeTimer(latest + LOOKBACK + RouteMetrics.WINDOW + LATENESS + 1);
    }
  }
  @Override public void initializeState(org.apache.flink.runtime.state.FunctionInitializationContext ctx) throws Exception {
    cutoffState = ctx.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>("cross-window-cutoff", Long.class));
    for (long value : cutoffState.get()) cutoff = Math.max(cutoff, value);
  }
  @Override public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext ctx) throws Exception {
    cutoffState.clear(); cutoffState.add(cutoff);
  }

  /** Mutates history with canonical first stop visits, after reading predecessor context. */
  static ObjectNode aggregate(List<String> values, List<String> history) throws Exception {
    ObjectNode result = RouteMetrics.aggregate(values);
    result.put("generation", "live-v3"); result.put("schema_version", 2);
    result.put("headway_lookback_seconds", LOOKBACK / 1000);
    Set<String> ids = new TreeSet<>();
    Map<RouteMetrics.StopVisit,JsonNode> seen = new HashMap<>();
    Map<String,JsonNode> previous = new HashMap<>();
    Comparator<JsonNode> order = Comparator.<JsonNode>comparingLong(e -> e.path("observed_at").asLong()).thenComparing(e -> e.path("event_id").asText());
    for (String value : history) {
      JsonNode event = LakehouseJob.parse(value);
      String stop = event.path("payload").path("vehicle_position").path("stop_id").asText();
      seen.put(new RouteMetrics.StopVisit(stop, event.path("trip_id").asText()), event);
      previous.merge(stop, event, (a,b) -> order.compare(a,b) > 0 ? a : b);
      ids.add(event.path("event_id").asText());
    }
    result.put("context_event_count", ids.size());
    var ordered = values.stream().map(LakehouseJob::parse).sorted(order).toList();
    var headways = new ArrayList<Double>();
    int arrivals = 0;
    for (JsonNode event : ordered) {
      ids.add(event.path("event_id").asText());
      JsonNode position = event.path("payload").path("vehicle_position");
      if (!position.path("current_status").asText().equals("STOPPED_AT") || !position.path("stop_id").isTextual() || position.path("stop_id").asText().isEmpty()) continue;
      String stop = position.path("stop_id").asText(), trip = event.path("trip_id").asText();
      var visit = new RouteMetrics.StopVisit(stop, trip);
      long observed = event.path("observed_at").asLong();
      JsonNode first = seen.get(visit);
      if (first != null && observed - first.path("observed_at").asLong() <= LOOKBACK) continue;
      JsonNode predecessor = previous.get(stop);
      if (predecessor != null && !predecessor.path("trip_id").asText().equals(trip)) {
        long gap = observed - predecessor.path("observed_at").asLong();
        if (gap <= LOOKBACK) headways.add(gap / 1000.0);
      }
      seen.put(visit, event); previous.put(stop, event);
      history.add(event.toString()); arrivals++;
    }
    result.put("arrival_count", arrivals); result.put("headway_count", headways.size());
    if (headways.isEmpty()) result.putNull("mean_headway_seconds");
    else result.put("mean_headway_seconds", headways.stream().mapToDouble(v -> v).average().orElseThrow());
    result.put("input_digest", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(String.join("\n", ids).getBytes(StandardCharsets.UTF_8))));
    return result;
  }
}
