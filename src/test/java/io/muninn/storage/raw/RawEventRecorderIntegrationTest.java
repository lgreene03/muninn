package io.muninn.storage.raw;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.config.StorageConfig;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * End to end on real infrastructure: events produced through muninn's own serializer,
 * recorded by the real consumer loop into real S3-compatible storage, then compared
 * byte for byte against an independent read of the broker.
 */
@Tag("integration")
@Testcontainers
class RawEventRecorderIntegrationTest {

    private static final String S3_CONFIG = """
            {"identities":[{"name":"it","credentials":[{"accessKey":"it","secretKey":"it-secret"}],
              "actions":["Admin","Read","List","Tagging","Write"]}]}""";

    @Container
    static final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static final GenericContainer<?> objectstore = new GenericContainer<>(DockerImageName.parse("chrislusf/seaweedfs:3.97"))
            .withCopyToContainer(Transferable.of(S3_CONFIG), "/etc/seaweedfs/s3.json")
            .withCommand("server", "-dir=/data", "-ip.bind=0.0.0.0", "-s3", "-s3.port=9000",
                    "-s3.config=/etc/seaweedfs/s3.json", "-master.volumeSizeLimitMB=64", "-volume.max=0")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/status").forPort(9000).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2));

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    @Test
    void everyRecordIsStoredExactlyOnceAndByteIdenticalToTheBroker() throws Exception {
        // The production factory, not a hand-built client. A hand-built client is what
        // hid the aws-chunked corruption: it validated a configuration production did
        // not run. This way the test covers exactly what the recorder will use.
        S3Client s3 = StorageConfig.newS3Client(new StorageConfig.S3Properties(
                "http://" + objectstore.getHost() + ":" + objectstore.getMappedPort(9000),
                "it", "it-secret", "us-east-1"));
        s3.createBucket(b -> b.bucket("muninn-raw"));

        List<String> topics = List.of("events.trade", "events.book.snapshot");
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            // Three partitions, as norse-stack provisions events.trade, so grouping and
            // per-partition commits are exercised rather than assumed.
            admin.createTopics(List.of(new NewTopic("events.trade", 3, (short) 1),
                    new NewTopic("events.book.snapshot", 3, (short) 1))).all().get();
        }

        produceThroughTheRealSerializer(120, 30);
        Map<String, byte[]> broker = readEverythingFromTheBroker(topics);
        assertThat(broker).hasSize(150);

        RawRecorderProperties props = new RawRecorderProperties(true, topics, "it-recorder", "muninn-raw",
                8L * 1024 * 1024, Duration.ofMillis(200), Duration.ofSeconds(2));
        RawEventRecorder recorder = new RawEventRecorder(
                new KafkaConsumer<>(consumerConfig("it-recorder")),
                new RawBatchWriter(new S3ObjectPutter(s3, "muninn-raw")),
                props, new SimpleMeterRegistry(), System::nanoTime);
        recorder.subscribe();

        Map<TopicPartition, Long> end = endOffsets(topics);
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!allCommitted(end) && System.nanoTime() < deadline) {
            recorder.pollOnce();
        }
        recorder.close();
        assertThat(allCommitted(end)).as("recorder committed every partition to its end offset").isTrue();

        Map<String, byte[]> stored = new HashMap<>();
        List<String> duplicates = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (S3Object o : s3.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket("muninn-raw").build()).contents()) {
            keys.add(o.key());
            byte[] gz = s3.getObjectAsBytes(GetObjectRequest.builder().bucket("muninn-raw").key(o.key()).build()).asByteArray();
            for (byte[] line : lines(gz)) {
                String id = coordinates(line);
                if (stored.put(id, valueBytes(line)) != null) {
                    duplicates.add(id);
                }
            }
        }

        assertThat(duplicates).as("no record stored twice").isEmpty();
        assertThat(stored.keySet()).as("every broker record stored").isEqualTo(broker.keySet());
        for (Map.Entry<String, byte[]> e : broker.entrySet()) {
            assertThat(stored.get(e.getKey())).as("byte-identical value for " + e.getKey()).isEqualTo(e.getValue());
        }

        // Precision survives the whole chain: real serializer, broker, recorder, storage.
        assertThat(stored.values().stream().anyMatch(v -> new String(v, StandardCharsets.UTF_8).contains("0.00150")))
                .as("a size quoted as 0.00150 keeps its trailing zero end to end").isTrue();
        assertThat(keys).allMatch(k -> k.contains("/source=binance.spot.v1/instrument=BTC-USDT/"));
        assertThat(keys).anyMatch(k -> k.startsWith("events.trade/")).anyMatch(k -> k.startsWith("events.book.snapshot/"));
    }

    private static void produceThroughTheRealSerializer(int trades, int books) throws Exception {
        Map<String, Object> cfg = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        try (KafkaProducer<String, MarketEvent> producer = new KafkaProducer<>(cfg)) {
            Instant t = Instant.parse("2026-10-01T14:00:00Z");
            for (int i = 0; i < trades; i++) {
                TradeEvent trade = new TradeEvent(UUIDv7.generate(), t.plusMillis(i), t.plusMillis(i + 5),
                        "binance.spot.v1", BTC, i, TradeEvent.CURRENT_SCHEMA_VERSION,
                        new BigDecimal("67523.45"), new BigDecimal("0.00150"),
                        i % 2 == 0 ? Side.BUY : Side.SELL, Long.toString(4_200_000 + i));
                // Spread across partitions explicitly; the symbol key alone would hash to one.
                producer.send(new ProducerRecord<>("events.trade", i % 3, "BTC-USDT", trade)).get();
            }
            for (int i = 0; i < books; i++) {
                OrderBookSnapshotEvent book = new OrderBookSnapshotEvent(UUIDv7.generate(), t.plusMillis(i * 100L),
                        t.plusMillis(i * 100L), "binance.spot.v1", BTC, i, OrderBookSnapshotEvent.CURRENT_SCHEMA_VERSION,
                        List.of(new PriceLevel(new BigDecimal("67523.40"), new BigDecimal("1.250"))),
                        List.of(new PriceLevel(new BigDecimal("67523.50"), new BigDecimal("0.800"))), 1);
                producer.send(new ProducerRecord<>("events.book.snapshot", i % 3, "BTC-USDT", book)).get();
            }
        }
    }

    private static Map<String, Object> consumerConfig(String group) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    /** The ground truth: what the broker actually holds, read independently of the recorder. */
    private static Map<String, byte[]> readEverythingFromTheBroker(List<String> topics) {
        Map<String, byte[]> out = new HashMap<>();
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(consumerConfig("it-ground-truth"))) {
            Map<TopicPartition, Long> end = endOffsets(topics);
            c.assign(end.keySet());
            c.seekToBeginning(end.keySet());
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (out.size() < end.values().stream().mapToLong(Long::longValue).sum() && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, byte[]> r : c.poll(Duration.ofMillis(500))) {
                    out.put(r.topic() + "/" + r.partition() + "/" + r.offset(), r.value());
                }
            }
        }
        return out;
    }

    private static Map<TopicPartition, Long> endOffsets(List<String> topics) {
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(consumerConfig("it-offsets"))) {
            List<TopicPartition> tps = topics.stream()
                    .flatMap(t -> c.partitionsFor(t).stream().map(p -> new TopicPartition(t, p.partition())))
                    .toList();
            return c.endOffsets(tps);
        }
    }

    private static boolean allCommitted(Map<TopicPartition, Long> end) {
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(consumerConfig("it-recorder"))) {
            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(Set.copyOf(end.keySet()));
            return end.entrySet().stream().allMatch(e -> e.getValue() == 0
                    || (committed.get(e.getKey()) != null && committed.get(e.getKey()).offset() == e.getValue()));
        }
    }

    private static List<byte[]> lines(byte[] gz) throws Exception {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(gz))) {
            byte[] all = in.readAllBytes();
            List<byte[]> out = new ArrayList<>();
            int start = 0;
            for (int i = 0; i < all.length; i++) {
                if (all[i] == '\n') {
                    out.add(Arrays.copyOfRange(all, start, i));
                    start = i + 1;
                }
            }
            return out;
        }
    }

    /** "topic/partition/offset" read from the envelope's leading fields. */
    private static String coordinates(byte[] line) {
        String head = new String(line, 0, Math.min(line.length, 200), StandardCharsets.UTF_8);
        String topic = between(head, "\"topic\":\"", "\"");
        String partition = between(head, "\"partition\":", ",");
        String offset = between(head, "\"offset\":", ",");
        return topic + "/" + partition + "/" + offset;
    }

    /**
     * The value bytes exactly as stored, cut out of the line without parsing, so the
     * comparison cannot be flattered by a parser normalising both sides the same way.
     * The value is the envelope's last field, so it runs from after ',"value":' to the
     * closing brace.
     */
    private static byte[] valueBytes(byte[] line) {
        byte[] marker = ",\"value\":".getBytes(StandardCharsets.UTF_8);
        int at = indexOf(line, marker);
        assertThat(at).as("line has a value field").isGreaterThanOrEqualTo(0);
        return Arrays.copyOfRange(line, at + marker.length, line.length - 1);
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static String between(String s, String from, String to) {
        int a = s.indexOf(from) + from.length();
        return s.substring(a, s.indexOf(to, a));
    }
}
