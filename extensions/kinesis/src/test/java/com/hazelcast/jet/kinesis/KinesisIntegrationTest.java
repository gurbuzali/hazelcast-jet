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

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KinesisIntegrationTest extends SimpleTestInClusterSupport {

    private static final int KEYS = 10;
    private static final int MEMBER_COUNT = 2;

    @Rule
    public LocalStackContainer localstack = new LocalStackContainer("0.12.1")
            .withServices(Service.KINESIS);

    @BeforeClass
    public static void beforeClass() {
//        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

        initialize(MEMBER_COUNT, null);
    }

    @Test
    public void test() {
        String testId = "test"; //todo: improve

        IMap<String, List<String>> results = instance().getMap(testId);

        AwsConfig awsConfig = getAwsConfig(localstack);
        AmazonKinesisAsync kinesis = awsConfig.buildClient();

        String streamName = "StockTradeStream";
        createStream(kinesis, streamName, 1);
        waitForStreamToActivate(kinesis, streamName);

        StreamSource<Entry<String, byte[]>> source = KinesisSources.kinesis(streamName)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                .build();

        Sink<Entry<String, List<String>>> sink = Sinks.map(results);

        instance().newJob(getPipeline(source, sink));

        int messages = 100;
        Map<String, List<String>> expectedMessages = sendMessages(kinesis, streamName, messages);
        assertTrueEventually(() -> assertEquals(expectedMessages, results));

    }

    private static AwsConfig getAwsConfig(LocalStackContainer localstack) {
        return new AwsConfig(
                "http://localhost:" + localstack.getMappedPort(4566),
                localstack.getRegion(),
                localstack.getAccessKey(),
                localstack.getSecretKey()
        );
    }

    private static void createStream(AmazonKinesisAsync kinesis, String streamName, int shardCount) {
        CreateStreamRequest request = new CreateStreamRequest();
        request.setShardCount(shardCount);
        request.setStreamName(streamName);
        kinesis.createStream(request);
    }

    private static Pipeline getPipeline(StreamSource<Entry<String, byte[]>> source,
                                        Sink<Entry<String, List<String>>> sink) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .groupingKey(Entry::getKey)
                .mapStateful((SupplierEx<List<String>>) ArrayList::new,
                        (s, k, e) -> {
                            s.add(new String(e.getValue(), Charset.defaultCharset()));
                            return Util.<String, List<String>>entry(k, new ArrayList<>(s));
                        })
                .writeTo(sink);
        return pipeline;
    }

    private static Map<String, List<String>> sendMessages(AmazonKinesisAsync kinesis, String streamName, int messages) {
        List<PutRecordsRequestEntry> requestEntries = new ArrayList<>(messages); //todo: 500 is the maximum limit
        Map<String, List<String>> sentMessages = new HashMap<>();

        for (int i = 0; i < messages; i++) {
            String key = Integer.toString(i % KEYS);
            String message = "Message " + i + " for key " + key;
            sentMessages.merge(key, Collections.singletonList(message), (l1, l2) -> {
                ArrayList<String> retList = new ArrayList<>();
                retList.addAll(l1);
                retList.addAll(l2);
                return retList;
            });
            requestEntries.add(putRecordRequestEntry(key, ByteBuffer.wrap(message.getBytes())));
        }

        PutRecordsResult response = kinesis.putRecords(putRecordsRequest(streamName, requestEntries));
        int failures = response.getFailedRecordCount();
        if (failures > 0) {
            fail("Sending messages to Kinesis failed: " + response);
        }

        return sentMessages;
    }

    private static void waitForStreamToActivate(AmazonKinesisAsync kinesis, String stream) {
        while (true) {
            StreamDescription description = kinesis.describeStream(stream).getStreamDescription();
            String status = description.getStreamStatus();
            if ("ACTIVE".equals(status)) {
                return;
            } else {
                System.out.println("Waiting for stream " + stream + " to be active ...");
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static PutRecordsRequestEntry putRecordRequestEntry(String key, ByteBuffer data) {
        PutRecordsRequestEntry request = new PutRecordsRequestEntry();
        request.setPartitionKey(key);
        request.setData(data);
        return request;
    }

    private static PutRecordsRequest putRecordsRequest(String stream, Collection<PutRecordsRequestEntry> entries) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setRecords(entries);
        request.setStreamName(stream);
        return request;
    }

}
