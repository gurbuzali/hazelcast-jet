package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.function.BiFunctionEx;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

public class BytesWritableProjection implements BiFunctionEx<NullWritable, BytesWritable, byte[]> {

    @Override
    public byte[] applyEx(NullWritable nullWritable, BytesWritable bytesWritable) {
        return bytesWritable.copyBytes();
    }
}
