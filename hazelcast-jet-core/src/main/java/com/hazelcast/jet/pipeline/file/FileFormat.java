package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;

import java.nio.file.Path;
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
     * Function that takes a Path on the local filesystem and maps it into a Stream of items.
     */
    FunctionEx<Path, Stream<T>> mapFn();

    /**
     * Apply configuration for this FileFormat to a Hadoop's org.apache.hadoop.mapreduce.Job
     *
     * TODO this has type Object in order to avoid a dependency on Hadoop in the core module
     * The implementations need to work with an instance of the Job, but it should not leak to this interface
     * It is not enough to just provide an InputFormat class, see e.g. AvroFileFormat which needs to set schema based on
     * config and
     * @param object instance of org.apache.hadoop.mapreduce.Job
     */
    void apply(Object object);

    /**
     * Function that takes (key, value) and maps it to an item produced by the source.
     *
     * The key and value are defined by the used FileFormat
     */
    BiFunctionEx<K, V, T> projectionFn();
}
