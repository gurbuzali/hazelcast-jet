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
package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class KinesisSourcePSupplier implements ProcessorSupplier {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final AwsConfig awsConfig;
    @Nonnull
    private final String stream;
    @Nonnull
    private final EventTimePolicy<? super Map.Entry<String, byte[]>> eventTimePolicy;
    @Nonnull
    private final HashRange hashRange;

    private transient AmazonKinesisAsync kinesis;

    public KinesisSourcePSupplier(
            @Nonnull AwsConfig awsConfig,
            @Nonnull String stream,
            @Nonnull EventTimePolicy<? super Map.Entry<String, byte[]>> eventTimePolicy,
            @Nonnull HashRange hashRange
    ) {
        this.awsConfig = awsConfig;
        this.stream = stream;
        this.eventTimePolicy = eventTimePolicy;
        this.hashRange = hashRange;
    }

    @Override
    public void init(@Nonnull Context context) {
        this.kinesis = awsConfig.buildClient();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new KinesisSourceP(kinesis, stream, eventTimePolicy, hashRange.partition(i, count)))
                .collect(toList());
    }
}
