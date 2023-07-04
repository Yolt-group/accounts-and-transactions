package com.yolt.accountsandtransactions.metrics.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.micrometer.core.instrument.Metrics;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Configurable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

/**
 * Kafka {@link ConsumerInterceptor} which measures the absence (e.a. nullable) of JSON fields in relation to the
 * presence of the JSON fields within the same JSON path.
 * <p/>
 * The goal is to "measure" how often a certain value (identified by its JSON path) is present/absent when receiving
 * a JSON message over kafka. For example, if the json is
 * <pre>
 *     {
 *         "a": {
 *             "a1: {
 *                 "a2": [
 *                      {
 *                          "a3": {
 *                              value: 1 // NON-NULL for key a.a1.a2.a3.value
 *                          }
 *                      },
 *                      {
 *                          "a3": {
 *                              value: null // NULL VALUE for key a.a1.a2.a3.value
 *                          }
 *                      }
 *                 ]
 *             }
 *         },
 *         "b" = 1,
 *         "c" = {
 *             "c1: "some test value"
 *         }
 *     }
 * </pre>
 * <p>
 * The keys above here are
 * <ul>
 *     <li>root.a</li>
 *     <li>root.a.a1</li>
 *     <li>root.a.a1.a2</li>
 *     <li>root.a.a1.a2.a3</li>
 *     <li>root.a.a1.a2.a3.value</li>
 *     <li>root.b</li>
 *     <li>root.c</li>
 *     <li>root.c.c1</li>
 * </ul>
 * <p>
 * Given a set of (uniform) {@link ConsumerRecords}, we count the number of "non-null" and "null" values.
 * In the above given example the key `root.a.a1.a2.a3.value` is seen twice, once with a value and once without a value (null).
 * The would give it a final "absence" of 0.5 as the value is absent for 50% of the values seen for this key
 * <p>
 * The measurements score is as followed:
 * <ul>
 *     <li>0.0 - Value was always present (non-null) for all occurrences of the json path in this set of {@link ConsumerRecords}</li>
 *     <li>between 0.0 and 0.1 - Value was present x % of the time in this set of {@link ConsumerRecords}</li>
 *     <li>0.1 - Value was never present (always null) for the given json path in this set of {@link ConsumerRecords}/li>
 * </ul>
 * <p>
 * Important to note. Only the values which are completely or partially absent are send to prometheus.
 * Values which are always present (in the current set of consumer records) and given a score of 1.0 are *not* send
 * to prometheus in order to cap the number of metrics send.
 */
@Slf4j
@Configurable
public class JsonKeyMetricKafkaRecordInterceptor implements ConsumerInterceptor<byte[], byte[]> {

    private static final String TYPE_ID_HEADER = "__TypeId__";

    private static final Set<String> REGISTERED_TYPES = Set.of(
            "com.yolt.providers.web.service.dto.IngestionRequestDTO",
            "com.yolt.accountsandtransactions.inputprocessing.AccountsAndTransactionsRequestDTO"
    );

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ConsumerRecords<byte[], byte[]> onConsume(ConsumerRecords<byte[], byte[]> records) {
        try {
            measure(records);
        } catch (Exception e) {
            log.error("Failed to consume Kafka records", e);
        }
        return records;
    }

    void measure(ConsumerRecords<byte[], byte[]> records) {

        Map<String, List<RawTypedConsumerRecord>> recordByType = stream(records.spliterator(), false)
                .map(record -> new RawTypedConsumerRecord(record.topic(), getTypeIdIfAny(record).orElse("undefined"), record))
                .filter(rawTypedConsumerRecord -> REGISTERED_TYPES.contains(rawTypedConsumerRecord.typeId))
                .collect(groupingBy(consumerRecordTypeAndValue -> shorten(consumerRecordTypeAndValue.typeId)));

        for (String type : recordByType.keySet()) {

            Map<String, List<JsonTypedConsumerRecord>> uniformRecordsByProvider = recordByType.get(type).stream()
                    .map(rawTypedRecord -> {
                        try {
                            JsonTypedConsumerRecord payload = JsonTypedConsumerRecord.builder()
                                    .rawTypedConsumerRecord(rawTypedRecord)
                                    .payload(objectMapper.readTree(rawTypedRecord.consumerRecord.value()))
                                    .build();
                            return Optional.of(payload);
                        } catch (IOException e) {
                            return Optional.<JsonTypedConsumerRecord>empty();
                        }
                    })
                    .flatMap(Optional::stream)
                    .collect(groupingBy(record -> getProviderIfAny(record.payload).orElse("")));

            for (Map.Entry<String, List<JsonTypedConsumerRecord>> uniformProviderRecords : uniformRecordsByProvider.entrySet()) {
                String provider = uniformProviderRecords.getKey();

                Map<String, Double> absentMetrics = sumPresence(uniformProviderRecords.getValue()).entrySet().stream()
                        .filter(e -> e.getValue() > 0.0)
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                absentMetrics.forEach((path, measurement) -> {
                    Metrics.summary("tmp_v1_kafka_consumer_json_field_absence",
                            "type", type,
                            "path", path,
                            "provider", provider
                    ).record(measurement);
                });
            }
        }
    }

    static Map<String, Double> sumPresence(final List<JsonTypedConsumerRecord> uniformRecords) {

        ArrayListMultimap<String, Boolean> collector = ArrayListMultimap.create();
        for (JsonTypedConsumerRecord record : uniformRecords) {
            walk("root", record.payload, collector);
        }

        Map<String, Double> absence = new HashMap<>();
        for (Map.Entry<String, Collection<Boolean>> statistics : collector.asMap().entrySet()) {
            String path = statistics.getKey();

            Map<Boolean, Long> availability = new HashMap<>();
            for (boolean isPresent : statistics.getValue()) {
                availability.compute(isPresent, (k, v) -> v == null ? 1L : v + 1L);
            }

            long present = availability.getOrDefault(true, 0L);
            long absent = availability.getOrDefault(false, 0L);
            long total = present + absent;

            if (total > 0) {
                // present 0, absent 1, total 1
                // present 1, absent 0, total 1
                // present 424, absent 86, total 510
                absence.putIfAbsent(path, (double) absent / total);
            }
        }
        return absence;
    }

    /**
     * Walk a JSON Tree and gather field presence/absence statistics
     *
     * @param path      the current path
     * @param node      the current node
     * @param collector the collector
     */
    static void walk(final String path,
                     final JsonNode node,
                     final Multimap<String, Boolean> collector) {
        if (node.isObject()) {
            for (Map.Entry<String, JsonNode> fieldNode : newArrayList(node.fields())) {
                walk(path + "." + fieldNode.getKey(), fieldNode.getValue(), collector);
            }
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;

            // if this node is an array, prefix the path with .[]
            for (int i = 0; i < arrayNode.size(); i++) {
                walk(path + ".[]", arrayNode.get(i), collector);
            }
        } else if (node.isValueNode()) {
            // add an occurrence of absence/presence to the collector
            collector.put(path, !(node.isNull()));
        }
    }

    private static Optional<String> getTypeIdIfAny(final ConsumerRecord<byte[], byte[]> record) {
        return stream(record.headers().spliterator(), false)
                .filter(header -> TYPE_ID_HEADER.equals(header.key()))
                .map(header -> new String(header.value(), StandardCharsets.US_ASCII))
                .findFirst();
    }

    private static Optional<String> getProviderIfAny(final JsonNode node) {
        JsonNode provider = node.findPath("provider");
        return provider.getNodeType() == JsonNodeType.MISSING
                ? Optional.empty()
                : Optional.of(provider.asText()); // empty string if not a ValueNode
    }

    static String shorten(String fullyQualifiedName) {
        return fullyQualifiedName.replaceAll("([a-z0-9_])[a-z0-9_]+\\.", "$1.");
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // NoOp
    }

    @Override
    public void close() {
        // NoOp
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // NoOp
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    static class RawTypedConsumerRecord {
        @NonNull
        public final String topic;
        @NonNull
        public final String typeId;
        @NonNull
        public final ConsumerRecord<byte[], byte[]> consumerRecord;
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    static class JsonTypedConsumerRecord {
        @NonNull
        public final JsonNode payload;
        @NonNull
        public final RawTypedConsumerRecord rawTypedConsumerRecord;
    }
}
