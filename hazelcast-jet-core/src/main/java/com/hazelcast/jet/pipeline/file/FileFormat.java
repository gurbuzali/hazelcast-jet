package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Specification of the file format
 *
 * @param <K>
 * @param <V>
 * @param <T>
 */
public interface FileFormat<K, V, T> {

    /**
     * Function that takes a Path on the local filesystem and maps it into a
     * Stream of items.
     */
    FunctionEx<Path, Stream<T>> localMapFn();

    /**
     * Options for configuring Hadoop job (InputFormat class and its
     * configuration)
     */
    Map<String, String> options();

    /**
     * Function that takes (key, value) and maps it to an item produced by the
     * source.
     * <p>
     * The key and value are defined by the used FileFormat
     */
    BiFunctionEx<K, V, T> projectionFn();
}
