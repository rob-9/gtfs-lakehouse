package io.gtfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/** Checkpointed normalized history, event-time deduplication, and late-event audit. */
public class LakehouseJob {
  static java.util.Properties producerConfig() {
    var properties = new java.util.Properties();
    properties.setProperty("transaction.timeout.ms", "180000");
    return properties;
  }
  static final ObjectMapper JSON = new ObjectMapper();
  static final OutputTag<String> LATE = new OutputTag<String>("late-events") {};
  static final Schema HISTORY = new Schema(
      Types.NestedField.required(1, "event_id", Types.StringType.get()),
      Types.NestedField.required(2, "observed_at", Types.LongType.get()),
      Types.NestedField.required(3, "record_json", Types.StringType.get()));

  static JsonNode parse(String value) {
    try { return JSON.readTree(value); }
    catch (Exception e) { throw new IllegalArgumentException("invalid event JSON", e); }
  }

  public static class AvroEvents extends AbstractDeserializationSchema<String> {
    @Override public String deserialize(byte[] bytes) throws java.io.IOException {
      try (var reader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(bytes), new GenericDatumReader<>())) {
        if (!reader.hasNext()) throw new java.io.IOException("empty event container");
        String event = reader.next().toString();
        if (reader.hasNext()) throw new java.io.IOException("expected one event per Kafka record");
        return event;
      }
    }
  }

  public static class Deduplicate extends KeyedProcessFunction<String, String, String>
      implements org.apache.flink.streaming.api.checkpoint.CheckpointedFunction {
    private final long retentionMillis;
    private transient ValueState<Boolean> seen;
    private transient org.apache.flink.api.common.state.ListState<Long> cutoffState;
    private long cutoff = Long.MIN_VALUE;
    public Deduplicate(long retentionMillis) { this.retentionMillis = retentionMillis; }
    @Override public void open(Configuration config) {
      seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen-event", Boolean.class));
    }
    @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
      long timestamp = parse(value).path("observed_at").asLong();
      long watermark = Math.max(cutoff, ctx.timerService().currentWatermark());
      cutoff = watermark;
      if (beyondRetention(timestamp, watermark, retentionMillis)) {
        ObjectNode audit = JSON.createObjectNode();
        audit.put("reason", "beyond_retention"); audit.set("event", parse(value));
        ctx.output(LATE, audit.toString()); return;
      }
      if (seen.value() == null) {
        seen.update(true); out.collect(value);
        ctx.timerService().registerEventTimeTimer(timestamp + retentionMillis);
      }
    }
    @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
      cutoff = Math.max(cutoff, ctx.timerService().currentWatermark());
      seen.clear();
    }
    @Override public void initializeState(org.apache.flink.runtime.state.FunctionInitializationContext ctx) throws Exception {
      cutoffState = ctx.getOperatorStateStore().getUnionListState(
          new org.apache.flink.api.common.state.ListStateDescriptor<>("admission-watermark", Long.class));
      for (long saved : cutoffState.get()) cutoff = Math.max(cutoff, saved);
    }
    @Override public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext ctx) throws Exception {
      cutoffState.clear(); cutoffState.add(cutoff);
    }
  }

  static boolean beyondRetention(long timestamp, long watermark, long retention) {
    // Before the first watermark Flink uses Long.MIN_VALUE; subtracting would overflow.
    return watermark != Long.MIN_VALUE && timestamp <= watermark - retention;
  }

  static void archive(org.apache.flink.streaming.api.datastream.DataStream<String> stream,
                      String name, CatalogLoader loader, org.apache.iceberg.catalog.Catalog catalog) {
    var identifier = TableIdentifier.of("gtfs", name);
    if (!catalog.tableExists(identifier)) catalog.createTable(identifier, HISTORY);
    var rows = stream.map(value -> {
      JsonNode record = parse(value);
      String id = record.hasNonNull("event_id") ? record.path("event_id").asText()
          : java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
              .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      long timestamp = record.path("observed_at").asLong(record.path("fetched_at").asLong(
          record.path("ingested_at").asLong(record.path("event").path("observed_at").asLong())));
      return (RowData) GenericRowData.of(StringData.fromString(id), timestamp, StringData.fromString(value));
    }).returns(RowData.class).uid(name + "-row-v1");
    FlinkSink.forRowData(rows).tableLoader(TableLoader.fromCatalog(loader, identifier))
        .uidPrefix(name + "-iceberg-v1").writeParallelism(1).append();
  }

  public static void main(String[] args) throws Exception {
    String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(60000);
    env.getCheckpointConfig().setCheckpointStorage("s3://checkpoints/flink/");
    env.getCheckpointConfig().enableExternalizedCheckpoints(
        org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    var source = KafkaSource.<String>builder().setBootstrapServers(brokers)
        .setTopics("gtfs.normalized.events").setGroupId("gtfs-normalization-v1")
        .setStartingOffsets(OffsetsInitializer.earliest()).setProperty("isolation.level", "read_committed")
        .setValueOnlyDeserializer(new AvroEvents()).build();
    var accepted = env.fromSource(source, WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
            .withTimestampAssigner((value, previous) -> parse(value).path("observed_at").asLong())
            .withIdleness(Duration.ofSeconds(15)), "normalized-events").uid("normalized-source-v1")
        .keyBy(value -> parse(value).path("event_id").asText())
        .process(new Deduplicate(120000)).uid("event-dedup-v1");

    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.URI, "http://catalog:8181");
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/");
    props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    props.put("s3.endpoint", "http://minio:9000"); props.put("s3.path-style-access", "true");
    props.put("s3.access-key-id", "lakehouse"); props.put("s3.secret-access-key", "local-lakehouse-secret");
    props.put("client.region", "us-east-1");
    var catalogLoader = CatalogLoader.custom("gtfs", props, new org.apache.hadoop.conf.Configuration(), "org.apache.iceberg.rest.RESTCatalog");
    var catalog = catalogLoader.loadCatalog();
    var rawSource = KafkaSource.<String>builder().setBootstrapServers(brokers)
        .setTopics("gtfs.raw.snapshots").setGroupId("gtfs-raw-archive-v1")
        .setStartingOffsets(OffsetsInitializer.earliest()).setProperty("isolation.level", "read_committed")
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();
    archive(env.fromSource(rawSource, WatermarkStrategy.noWatermarks(), "raw-manifests").uid("raw-source-v1"),
        "raw_feed_snapshots", catalogLoader, catalog);
    var auditSource = KafkaSource.<String>builder().setBootstrapServers(brokers)
        .setTopics("gtfs.dead_letter", "gtfs.late.events").setGroupId("gtfs-audit-archive-v1")
        .setStartingOffsets(OffsetsInitializer.earliest()).setProperty("isolation.level", "read_committed")
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();
    archive(env.fromSource(auditSource, WatermarkStrategy.noWatermarks(), "audit-records").uid("audit-source-v1"),
        "rejected_events", catalogLoader, catalog);
    var identifier = TableIdentifier.of("gtfs", "normalized_events");
    if (!catalog.tableExists(identifier)) catalog.createTable(identifier, HISTORY);
    var loader = TableLoader.fromCatalog(catalogLoader, identifier);
    var rows = accepted.map(value -> (RowData) GenericRowData.of(
        StringData.fromString(parse(value).path("event_id").asText()),
        parse(value).path("observed_at").asLong(), StringData.fromString(value)))
        .returns(RowData.class).uid("normalized-row-v1");
    FlinkSink.forRowData(rows).tableLoader(loader).uidPrefix("normalized-iceberg-v1").writeParallelism(1).append();
    var scheduleSource = KafkaSource.<String>builder().setBootstrapServers(brokers)
        .setTopics("gtfs.schedule.versions").setGroupId("gtfs-schedules-v1")
        .setStartingOffsets(OffsetsInitializer.earliest()).setProperty("isolation.level", "read_committed")
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();
    var schedules = env.fromSource(scheduleSource, WatermarkStrategy.<String>noWatermarks()
        .withIdleness(Duration.ofSeconds(15)), "schedule-manifests").uid("schedule-source-v1");
    var enriched = accepted.keyBy(value -> parse(value).path("agency_id").asText())
        .connect(schedules.broadcast(ScheduleJoin.VERSIONS)).process(new ScheduleJoin()).uid("schedule-join-v1");
    enriched.filter(value -> !parse(value).path("unmatched_reason").isNull()).uid("unmatched-only-v1")
        .sinkTo(KafkaSink.<String>builder().setBootstrapServers(brokers).setKafkaProducerConfig(producerConfig())
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE).setTransactionalIdPrefix("gtfs-unmatched-v1-")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic("gtfs.dead_letter")
            .setValueSerializationSchema(new SimpleStringSchema()).build()).build()).uid("unmatched-kafka-v1");
    var enrichedId = TableIdentifier.of("gtfs", "enriched_events");
    if (!catalog.tableExists(enrichedId)) catalog.createTable(enrichedId, HISTORY);
    var enrichedRows = enriched.map(value -> (RowData) GenericRowData.of(
        StringData.fromString(parse(value).path("event_id").asText()),
        parse(value).path("observed_at").asLong(), StringData.fromString(value))).returns(RowData.class).uid("enriched-row-v1");
    FlinkSink.forRowData(enrichedRows).tableLoader(TableLoader.fromCatalog(catalogLoader, enrichedId)).uidPrefix("enriched-iceberg-v1").writeParallelism(1).append();
    var metrics = enriched.filter(value -> parse(value).path("unmatched_reason").isNull()).uid("matched-only-v1")
        .keyBy(RouteMetrics::key).process(new RouteMetrics()).uid("route-windows-v1");
    archive(metrics.getSideOutput(RouteMetrics.INPUTS), "metric_inputs", catalogLoader, catalog);
    metrics.sinkTo(KafkaSink.<String>builder().setBootstrapServers(brokers)
        .setKafkaProducerConfig(producerConfig())
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE).setTransactionalIdPrefix("gtfs-metrics-v1-")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic("gtfs.route.metrics")
            .setValueSerializationSchema(new SimpleStringSchema()).build()).build()).uid("metrics-kafka-v1");
    var metricId = TableIdentifier.of("gtfs", "route_window_metrics");
    if (!catalog.tableExists(metricId)) catalog.createTable(metricId, HISTORY);
    var metricRows = metrics.map(value -> (RowData) GenericRowData.of(
        StringData.fromString(RouteMetrics.key(value)), parse(value).path("window_start").asLong(),
        StringData.fromString(value))).returns(RowData.class).uid("metric-row-v1");
    FlinkSink.forRowData(metricRows).tableLoader(TableLoader.fromCatalog(catalogLoader, metricId)).uidPrefix("metrics-iceberg-v1").writeParallelism(1).append();
    accepted.getSideOutput(LATE).union(metrics.getSideOutput(LATE)).sinkTo(KafkaSink.<String>builder().setBootstrapServers(brokers)
        .setKafkaProducerConfig(producerConfig())
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE).setTransactionalIdPrefix("gtfs-late-v1-")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic("gtfs.late.events")
            .setValueSerializationSchema(new SimpleStringSchema()).build()).build()).uid("late-audit-v1");
    env.execute("GTFS Streaming Lakehouse");
  }
}
