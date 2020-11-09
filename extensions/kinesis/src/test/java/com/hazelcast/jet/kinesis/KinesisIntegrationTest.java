/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.HashRange;
import com.hazelcast.jet.kinesis.impl.KinesisUtil;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.MapUtil.entry;
import static com.hazelcast.jet.kinesis.impl.KinesisUtil.hashRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KinesisIntegrationTest extends JetTestSupport {

    //todo: tests are slow... why?

    @ClassRule
    public static final LocalStackContainer LOCALSTACK = new LocalStackContainer("0.12.1")
            .withServices(Service.KINESIS);

    private static final int KEYS = 10;
    private static final int MEMBER_COUNT = 2;
    private static final int MESSAGES = 25_000;
    private static final String STREAM = "TestStream";
    private static final String RESULTS = "Results";

    private static AwsConfig AWS_CONFIG;
    private static AmazonKinesisAsync KINESIS;

    private JetInstance jet;
    private IMap<String, List<String>> results;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        //todo: force jackson versions to what we use (2.11.x) and have the resulting issue
        // fixed by Localstack (https://github.com/localstack/localstack/issues/3208)

        AWS_CONFIG = new AwsConfig(
                "http://localhost:" + LOCALSTACK.getMappedPort(4566),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey()
        );
        KINESIS = AWS_CONFIG.buildClient();
    }

    @Before
    public void before() {
        jet = createJetMembers(MEMBER_COUNT)[0];
        results = jet.getMap(RESULTS);
    }

    @After
    public void after() {
        deleteStream();

        results.clear();
        results.destroy();
    }

    @Test
    public void staticStream_1Shard() {
        staticStream(1);
    }

    @Test
    public void staticStream_2Shards() {
        staticStream(2);
    }

    @Test
    public void staticStream_50Shards() {
        staticStream(50);
    }

    private void staticStream(int shardCount) {
        createStream(shardCount);
        waitForStreamToActivate();

        StreamSource<Entry<String, byte[]>> source = kinesisSource();

        Sink<Entry<String, List<String>>> sink = Sinks.map(results);

        jet.newJob(getPipeline(source, sink));

        Map<String, List<String>> expectedMessages = sendMessages(true);
        assertMessages(expectedMessages, results, true);
    }

    @Test
    public void dynamicStream_2Shards_mergeBeforeData() {
        int shardCount = 2;

        createStream(shardCount);
        waitForStreamToActivate();

        List<String> shards = getActiveShards().stream()
                .map(Shard::getShardId)
                .collect(Collectors.toList());
        mergeShards(shards.get(0), shards.get(1));

        StreamSource<Entry<String, byte[]>> source = kinesisSource();

        Sink<Entry<String, List<String>>> sink = Sinks.map(results);

        jet.newJob(getPipeline(source, sink));

        Map<String, List<String>> expectedMessages = sendMessages(true);
        assertMessages(expectedMessages, results, true);
    }

    @Test
    public void dynamicStream_2Shards_mergeDuringData() {
        dynamicStream_mergesDuringData(2, 1);
    }

    @Test
    public void dynamicStream_50Shards_mergesDuringData() {
        //important to test with more shards than can fit in a single list shards response
         dynamicStream_mergesDuringData(50, 5);
    }

    private void dynamicStream_mergesDuringData(int shards, int merges) {
        createStream(shards);
        waitForStreamToActivate();

        StreamSource<Entry<String, byte[]>> source = kinesisSource();

        Sink<Entry<String, List<String>>> sink = Sinks.map(results);

        jet.newJob(getPipeline(source, sink));

        Map<String, List<String>> expectedMessages = sendMessages(false);

        //wait for some data to start coming out of the pipeline, before starting the merging
        assertTrueEventually(() -> assertFalse(results.isEmpty()));

        List<Shard> oldShards = Collections.emptyList();
        for (int i = 0; i < merges; i++) {
            Set<String> oldShardIds = oldShards.stream().map(Shard::getShardId).collect(Collectors.toSet());
            List<Shard> currentShards = getActiveShards();
            List<Shard> newShards = currentShards.stream()
                    .filter(shard -> !oldShardIds.contains(shard.getShardId()))
                    .collect(Collectors.toList());
            assertTrue(newShards.size() >= 1);
            oldShards = currentShards;

            Collections.shuffle(newShards);
            Tuple2<String, String> adjacentPair = findAdjacentPair(newShards.get(0), currentShards);
            mergeShards(adjacentPair.f0(), adjacentPair.f1());
            waitForStreamToActivate();
        }

        assertMessages(expectedMessages, results, false);
    }

    private Map<String, List<String>> sendMessages(boolean join) {
        List<Entry<String, String>> msgEntryList = IntStream.range(0, MESSAGES)
                .boxed()
                .map(i -> entry(Integer.toString(i % KEYS), i))
                .map(e -> entry(e.getKey(), String.format("Message %09d for key %s", e.getValue(), e.getKey())))
                .collect(Collectors.toList());

        BatchSource<Entry<String, byte[]>> source = TestSources.items(msgEntryList.stream()
                .map(e1 -> entry(e1.getKey(), e1.getValue().getBytes()))
                .collect(Collectors.toList()));
        Sink<Entry<String, byte[]>> sink = kinesisSink();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .writeTo(sink);

        Job job = jet.newJob(pipeline);

        Map<String, List<String>> retMap = toMap(msgEntryList);

        if (join) {
            job.join();
        }

        return retMap;
    }

    private static Tuple2<String, String> findAdjacentPair(Shard shard, List<Shard> allShards) {
        HashRange shardRange = hashRange(shard);
        for (Shard examinedShard : allShards) {
            HashRange examinedRange = hashRange(examinedShard);
            if (shardRange.isAdjacent(examinedRange)) {
                if (shardRange.getMinInclusive().compareTo(examinedRange.getMinInclusive()) <= 0) {
                    return Tuple2.tuple2(shard.getShardId(), examinedShard.getShardId());
                } else {
                    return Tuple2.tuple2(examinedShard.getShardId(), shard.getShardId());
                }
            }
        }
        throw new IllegalStateException("There must be an adjacent shard");
    }

    private static void createStream(int shardCount) {
        CreateStreamRequest request = new CreateStreamRequest();
        request.setShardCount(shardCount);
        request.setStreamName(STREAM);
        KINESIS.createStream(request);
    }

    private static void deleteStream() {
        KINESIS.deleteStream(STREAM);
        assertTrueEventually(() -> assertFalse(KINESIS.listStreams().isHasMoreStreams()));
    }

    private static void waitForStreamToActivate() {
        KinesisUtil.waitForStreamToActivate(KINESIS, STREAM);
    }

    private static Pipeline getPipeline(StreamSource<Entry<String, byte[]>> source,
                                        Sink<Entry<String, List<String>>> sink) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .groupingKey(Entry::getKey)
                .mapStateful((SupplierEx<List<String>>) ArrayList::new,
                        (s, k, e) -> {
                            String m = new String(e.getValue(), Charset.defaultCharset());
                            s.add(m);
                            return Util.<String, List<String>>entry(k, new ArrayList<>(s));
                        })
                .writeTo(sink);
        return pipeline;
    }

    private static void mergeShards(String shard1, String shard2) {
        MergeShardsRequest request = new MergeShardsRequest();
        request.setStreamName(STREAM);
        request.setShardToMerge(shard1);
        request.setAdjacentShardToMerge(shard2);

        System.out.println("Merging " + shard1 + " with " + shard2);
        KINESIS.mergeShards(request);
    }

    private static List<Shard> getActiveShards() {
        return KinesisUtil.getActiveShards(KINESIS, STREAM);
    }

    private static StreamSource<Entry<String, byte[]>> kinesisSource() {
        return KinesisSources.kinesis(STREAM)
                .withEndpoint(AWS_CONFIG.getEndpoint())
                .withRegion(AWS_CONFIG.getRegion())
                .withCredentials(AWS_CONFIG.getAccessKey(), AWS_CONFIG.getSecretKey())
                .build();
    }

    private static Sink<Entry<String, byte[]>> kinesisSink() {
        return KinesisSinks.kinesis(STREAM)
                .withEndpoint(AWS_CONFIG.getEndpoint())
                .withRegion(AWS_CONFIG.getRegion())
                .withCredentials(AWS_CONFIG.getAccessKey(), AWS_CONFIG.getSecretKey())
                .build();
    }

    private static void assertMessages(
            Map<String, List<String>> expected,
            IMap<String, List<String>> actual,
            boolean checkOrder
    ) {
        assertTrueEventually(() -> {
            assertEquals(getKeySetsDifferDescription(expected, actual), expected.keySet(), actual.keySet());

            for (Entry<String, List<String>> entry : expected.entrySet()) {
                String key = entry.getKey();
                List<String> expectedMessages = entry.getValue();

                List<String> actualMessages = actual.get(key);
                if (!checkOrder) {
                    actualMessages = new ArrayList<>(actualMessages);
                    actualMessages.sort(String::compareTo);
                }
                assertEquals(getMessagesDifferDescription(key, expectedMessages, actualMessages),
                        expectedMessages, actualMessages);
            }
        });
    }

    @Nonnull
    private static String getKeySetsDifferDescription(Map<String, List<String>> expected,
                                                      IMap<String, List<String>> actual) {
        return "Key sets differ!" +
                "\n\texpected: " + new TreeSet<>(expected.keySet()) +
                "\n\t  actual: " + new TreeSet<>(actual.keySet());
    }

    @Nonnull
    private static String getMessagesDifferDescription(String key, List<String> expected, List<String> actual) {
        StringBuilder sb = new StringBuilder()
                .append("Messages for key ").append(key).append(" differ!")
                .append("\n\texpected: ").append(expected.size())
                .append("\n\t  actual: ").append(actual.size());

        for (int i = 0; i < Math.min(expected.size(), actual.size()); i++) {
            if (!expected.get(i).equals(actual.get(i))) {
                sb.append("\n\tfirst difference at index: ").append(i);
                sb.append("\n\t\texpected: ").append(expected.get(i));
                for (int j = i + 1; j < Math.min(i + 10, expected.size()); j++) {
                    sb.append(", ").append(expected.get(j));
                }
                sb.append("\n\t\t  actual: ").append(actual.get(i));
                for (int j = i + 1; j < Math.min(i + 10, actual.size()); j++) {
                    sb.append(", ").append(actual.get(j));
                }
                break;
            }
        }

        return sb.toString();
    }

    private static Map<String, List<String>> toMap(List<Entry<String, String>> entryList) {
        return entryList.stream()
                .collect(Collectors.toMap(
                        Entry::getKey,
                        e -> Collections.singletonList(e.getValue()),
                        (l1, l2) -> {
                            ArrayList<String> retList = new ArrayList<>();
                            retList.addAll(l1);
                            retList.addAll(l2);
                            return retList;
                        }
                ));
    }

}
