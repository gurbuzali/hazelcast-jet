package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.function.BiFunctionEx;

public class ValueToStringProjection implements BiFunctionEx<Object, Object, String> {

    @Override
    public String applyEx(Object key, Object value) {
        return value.toString();
    }
}
