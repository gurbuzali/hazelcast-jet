package com.hazelcast.jet.pipeline.file;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CsvFileFormat<T> implements FileFormat<NullWritable, T, T> {

    private final Class<T> clazz;
    private final CsvMapper mapper;
    private final ObjectReader reader;

    public CsvFileFormat(Class<T> clazz) {
        this.clazz = clazz;
        mapper = new CsvMapper();

        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        reader = mapper.readerFor(clazz).with(schema);
    }

    @Override
    public FunctionEx<Path, Stream<T>> mapFn() {
        ObjectReader thisObjectReader = this.reader;
        return (path) -> StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        thisObjectReader.readValues(path.toFile()),
                        Spliterator.ORDERED)
                , false);
    }

    @Override
    public void apply(Object object) {
        if (object instanceof Job) {
            Job job = (Job) object;

            try {

                @SuppressWarnings("unchecked")
                Class<? extends InputFormat<?, ?>> format = (Class<? extends InputFormat<?, ?>>)
                        Thread.currentThread()
                              .getContextClassLoader()
                              .loadClass("com.hazelcast.jet.hadoop.impl.CsvInputFormat");
                job.setInputFormatClass(format);
                job.getConfiguration().set("csv.bean.class", clazz.getCanonicalName());

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public BiFunctionEx<NullWritable, T, T> projectionFn() {
        return (k, v) -> v;
    }
}
