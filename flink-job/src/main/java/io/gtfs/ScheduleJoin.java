package io.gtfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** Fixture-scale schedule broadcast with bounded waiting for committed versions. */
public class ScheduleJoin extends KeyedBroadcastProcessFunction<String, String, String, String> {
  static final MapStateDescriptor<String, String> VERSIONS = new MapStateDescriptor<>("schedule-versions", String.class, String.class);
  private transient ListState<String> waiting;
  @Override public void open(Configuration config) {
    waiting = getRuntimeContext().getListState(new ListStateDescriptor<>("waiting-schedule", String.class));
  }
  @Override public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
    JsonNode manifest = LakehouseJob.parse(value);
    String key = manifest.path("agency_id").asText() + "/" + manifest.path("schedule_version").asText();
    ctx.getBroadcastState(VERSIONS).put(key, value);
  }
  static JsonNode choose(String agency, long observed, Iterable<java.util.Map.Entry<String,String>> manifests) {
    JsonNode selected = null;
    long selectedTime = Long.MIN_VALUE;
    for (var entry : manifests) {
      JsonNode candidate = LakehouseJob.parse(entry.getValue());
      long start = Instant.parse(candidate.path("effective_from").asText()).toEpochMilli();
      if (candidate.path("agency_id").asText().equals(agency) && start <= observed && start > selectedTime) {
        selected = candidate; selectedTime = start;
      }
    }
    return selected;
  }
  static ObjectNode enrich(JsonNode event, JsonNode manifest) {
    ObjectNode result = event.deepCopy();
    result.put("schedule_version", manifest.path("schedule_version").asText());
    JsonNode trip = null;
    for (JsonNode row : manifest.path("schedule").path("trips"))
      if (row.path("trip_id").asText().equals(event.path("trip_id").asText())) trip = row;
    if (trip == null) { result.put("unmatched_reason", "unknown_trip"); return result; }
    String startDate = event.path("payload").path("trip").path("start_date").asText("");
    if (startDate.isEmpty()) { result.put("unmatched_reason", "ambiguous_service_date"); return result; }
    LocalDate day;
    try { day = LocalDate.parse(startDate, DateTimeFormatter.BASIC_ISO_DATE); }
    catch (Exception e) { result.put("unmatched_reason", "invalid_service_date"); return result; }
    String service = trip.path("service_id").asText();
    boolean active = false;
    String weekday = day.getDayOfWeek().name().toLowerCase(java.util.Locale.ROOT);
    for (JsonNode row : manifest.path("schedule").path("calendar")) {
      if (row.path("service_id").asText().equals(service)
          && startDate.compareTo(row.path("start_date").asText()) >= 0
          && startDate.compareTo(row.path("end_date").asText()) <= 0
          && row.path(weekday).asText().equals("1")) active = true;
    }
    for (JsonNode row : manifest.path("schedule").path("calendar_dates"))
      if (row.path("service_id").asText().equals(service) && row.path("date").asText().equals(startDate))
        active = row.path("exception_type").asText().equals("1");
    if (!active) { result.put("unmatched_reason", "inactive_service"); return result; }
    result.put("route_id", trip.path("route_id").asText());
    result.put("direction_id", trip.path("direction_id").asInt(-1));
    result.put("service_date", day.toString()); result.putNull("unmatched_reason");
    return result;
  }
  @Override public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
    JsonNode event = LakehouseJob.parse(value);
    JsonNode manifest = choose(event.path("agency_id").asText(), event.path("observed_at").asLong(), ctx.getBroadcastState(VERSIONS).immutableEntries());
    if (manifest != null) { out.collect(enrich(event, manifest).toString()); return; }
    var pending = new ArrayList<String>(); waiting.get().forEach(pending::add);
    if (pending.size() >= 1000) {
      ObjectNode audit = (ObjectNode) event; audit.put("unmatched_reason", "schedule_buffer_full");
      out.collect(audit.toString()); return;
    }
    ObjectNode entry = LakehouseJob.JSON.createObjectNode();
    entry.put("deadline", ctx.timerService().currentProcessingTime() + 60000); entry.put("value", value);
    waiting.add(entry.toString());
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000);
  }
  @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    var remaining = new ArrayList<String>();
    for (String pending : waiting.get()) {
      JsonNode entry = LakehouseJob.parse(pending), event = LakehouseJob.parse(entry.path("value").asText());
      JsonNode manifest = choose(event.path("agency_id").asText(), event.path("observed_at").asLong(), ctx.getBroadcastState(VERSIONS).immutableEntries());
      if (manifest != null) out.collect(enrich(event, manifest).toString());
      else if (entry.path("deadline").asLong() <= timestamp) {
        ObjectNode audit = (ObjectNode) event; audit.put("unmatched_reason", "schedule_unavailable"); out.collect(audit.toString());
      } else remaining.add(pending);
    }
    waiting.update(remaining);
    if (!remaining.isEmpty()) ctx.timerService().registerProcessingTimeTimer(timestamp + 1000);
  }
}
