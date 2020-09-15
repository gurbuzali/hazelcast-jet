package com.hazelcast.jet.pipeline.file;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.hadoop.io.NullWritable;

import java.io.InputStream;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CsvFileFormat<T> extends AbstractFileFormat<NullWritable, T, T> implements FileFormat<NullWritable, T, T> {

    public static final String CSV_INPUT_FORMAT_BEAN_CLASS = "csv.bean.class";

    private final Class<T> clazz;

    public CsvFileFormat(Class<T> clazz) {
        this.clazz = clazz;

        withOption(INPUT_FORMAT_CLASS, "com.hazelcast.jet.hadoop.impl.CsvInputFormat");
        withOption(CSV_INPUT_FORMAT_BEAN_CLASS, clazz.getCanonicalName());
    }

    @Override
    public FunctionEx<InputStream, Stream<T>> mapInputStreamFn() {
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        CsvMapper mapper = new CsvMapper();
        ObjectReader reader = mapper.readerFor(clazz).with(schema);
        return is -> StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        reader.readValues(is),
                        Spliterator.ORDERED),
                false);
    }

    @Override
    public BiFunctionEx<NullWritable, T, T> projectionFn() {
        return (k, v) -> v;
    }
}
