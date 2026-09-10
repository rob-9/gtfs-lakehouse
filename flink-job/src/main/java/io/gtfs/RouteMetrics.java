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

/** Final five-minute windows, retaining only the latest prediction per trip/stop. */
public class RouteMetrics extends KeyedProcessFunction<String,String,String> {
  static final long WINDOW = 300000;
  private transient ListState<String> events;
  @Override public void open(Configuration config) {
    events = getRuntimeContext().getListState(new ListStateDescriptor<>("route-input", String.class));
  }
  static String key(String value) {
    JsonNode e = LakehouseJob.parse(value);
    return LakehouseJob.JSON.createArrayNode().add(e.path("agency_id").asText())
        .add(e.path("route_id").asText()).add(e.path("direction_id").asInt(-1))
        .add(e.path("service_date").asText())
        .add(Math.floorDiv(e.has("observed_at") ? e.path("observed_at").asLong() : e.path("window_start").asLong(), WINDOW)).toString();
  }
  record Prediction(String trip, String stop, String sequence) {}
  record StopVisit(String stop, String trip) {}
  @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
    long start = Math.floorDiv(LakehouseJob.parse(value).path("observed_at").asLong(), WINDOW) * WINDOW;
    if (start + WINDOW + 120000 <= ctx.timerService().currentWatermark()) {
      ctx.output(LakehouseJob.LATE, value); return;
    }
    events.add(value);
    ctx.timerService().registerEventTimeTimer(start + WINDOW + 120000);
  }
  @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    var values = new ArrayList<String>(); events.get().forEach(values::add);
    if (!values.isEmpty()) out.collect(aggregate(values).toString());
    events.clear();
  }
  static ObjectNode aggregate(List<String> values) throws Exception {
    values = new ArrayList<>(values);
    values.sort(Comparator.<String>comparingLong(v -> LakehouseJob.parse(v).path("observed_at").asLong())
        .thenComparing(v -> LakehouseJob.parse(v).path("event_id").asText()));
    JsonNode first = LakehouseJob.parse(values.get(0));
    Map<Prediction, Integer> latest = new HashMap<>();
    Set<String> vehicles = new HashSet<>(), ids = new TreeSet<>(), canceled = new HashSet<>();
    Map<StopVisit, Long> arrivals = new HashMap<>();
    for (String value : values) {
      JsonNode event = LakehouseJob.parse(value);
      ids.add(event.path("event_id").asText());
      if (!event.path("vehicle_id").isNull()) vehicles.add(event.path("vehicle_id").asText());
      String trip = event.path("trip_id").asText();
      String relationship = event.path("payload").path("trip").path("schedule_relationship").asText();
      if (relationship.equals("CANCELED")) { canceled.add(trip); latest.keySet().removeIf(k -> k.trip().equals(trip)); }
      else if (event.path("payload").path("trip_update").isObject()) {
        canceled.remove(trip);
        for (JsonNode stop : event.path("payload").path("trip_update").path("stop_time_updates")) {
          Prediction sample = new Prediction(trip, stop.path("stop_id").asText(null), stop.path("stop_sequence").asText(null));
          if (stop.path("arrival_delay").isNumber()) latest.put(sample, stop.path("arrival_delay").asInt());
          else latest.remove(sample);
        }
      }
      JsonNode position = event.path("payload").path("vehicle_position");
      if (position.path("current_status").asText().equals("STOPPED_AT") && !position.path("stop_id").isNull())
        arrivals.merge(new StopVisit(position.path("stop_id").asText(), trip), event.path("observed_at").asLong(), Math::min);
    }
    var delays = new ArrayList<>(latest.values()); Collections.sort(delays);
    var byStop = new HashMap<String,List<Long>>();
    arrivals.forEach((k,v) -> byStop.computeIfAbsent(k.stop(), x -> new ArrayList<>()).add(v));
    var headways = new ArrayList<Double>();
    byStop.values().forEach(times -> { Collections.sort(times); for(int i=1;i<times.size();i++) headways.add((times.get(i)-times.get(i-1))/1000.0); });
    ObjectNode result = LakehouseJob.JSON.createObjectNode();
    result.put("schema_version", 1); result.put("generation", "live-v1");
    result.put("agency_id", first.path("agency_id").asText()); result.put("route_id", first.path("route_id").asText());
    result.put("direction_id", first.path("direction_id").asInt(-1)); result.put("service_date", first.path("service_date").asText());
    long start = Math.floorDiv(first.path("observed_at").asLong(), WINDOW) * WINDOW;
    result.put("window_start", start); result.put("window_end", start + WINDOW); result.put("version", 1);
    result.put("input_digest", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(String.join("\n", ids).getBytes(StandardCharsets.UTF_8))));
    result.put("vehicle_count", vehicles.size()); result.put("prediction_count", delays.size()); result.put("canceled_trip_count", canceled.size());
    result.put("arrival_count", arrivals.size()); result.put("headway_count", headways.size());
    if (delays.isEmpty()) { result.putNull("mean_delay_seconds"); result.putNull("p50_delay_seconds"); result.putNull("p95_delay_seconds"); result.putNull("adherence_rate"); }
    else {
      result.put("mean_delay_seconds", delays.stream().mapToInt(i -> i).average().orElseThrow());
      result.put("p50_delay_seconds", delays.get((int)Math.ceil(delays.size() * 0.50)-1));
      result.put("p95_delay_seconds", delays.get((int)Math.ceil(delays.size() * 0.95)-1));
      result.put("adherence_rate", delays.stream().filter(i -> i >= -60 && i <= 300).count() / (double)delays.size());
    }
    if (headways.isEmpty()) result.putNull("mean_headway_seconds");
    else result.put("mean_headway_seconds", headways.stream().mapToDouble(i -> i).average().orElseThrow());
    return result;
  }
}
