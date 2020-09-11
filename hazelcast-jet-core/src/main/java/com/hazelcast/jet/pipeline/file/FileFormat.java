package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.stream.Stream;

public interface FileFormat<K, V, T> {

    FunctionEx<Path, Stream<T>> mapFn();

    void apply(Object object);

    BiFunctionEx<K, V, T> projectionFn();
}
