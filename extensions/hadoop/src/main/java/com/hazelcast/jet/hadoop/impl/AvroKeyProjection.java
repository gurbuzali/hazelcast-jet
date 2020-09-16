package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.function.BiFunctionEx;
import org.apache.avro.mapred.AvroKey;

public class AvroKeyProjection<T> implements BiFunctionEx<AvroKey<T>, Object, T> {

    @Override
    public T applyEx(AvroKey<T> avroKey, Object value) {
        return avroKey.datum();
    }
}
