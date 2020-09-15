package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.nio.file.Path;
import java.util.stream.Stream;

public class ParquetFileFormat<T> extends AbstractFileFormat<String, T, T>
        implements FileFormat<String, T, T> {

    public ParquetFileFormat() {
        withOption(INPUT_FORMAT_CLASS, AvroParquetInputFormat.class.getCanonicalName());
    }

    @Override
    public FunctionEx<Path, Stream<T>> localMapFn() {
        throw new UnsupportedOperationException("Reading Parquet files is not supported in local filesystem mode. " +
                "Use Jet Hadoop module with FileSourceBuilder.useHadoopForLocalFiles option instead.");
    }

    @Override
    public BiFunctionEx<String, T, T> projectionFn() {
        return (k, v) -> v;
    }
}
