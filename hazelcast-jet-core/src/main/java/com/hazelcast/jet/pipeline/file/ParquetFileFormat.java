package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.nio.file.Path;
import java.util.stream.Stream;

public class ParquetFileFormat<T> implements FileFormat<String, T, T>  {

    @Override
    public FunctionEx<Path, Stream<T>> mapFn() {
        throw new UnsupportedOperationException("Reading Parquet files is not supported in local filesystem mode. " +
                "Use Jet Hadoop module with FileSourceBuilder.useHadoopForLocalFiles option instead.");
    }

    @Override
    public void apply(Object object) {
        if (object instanceof Job) {
            Job job = (Job) object;
            job.setInputFormatClass(AvroParquetInputFormat.class);
        }
    }

    @Override
    public BiFunctionEx<String, T, T> projectionFn() {
        return (k, v) -> v;
    }
}
