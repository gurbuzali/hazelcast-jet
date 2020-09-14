package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.json.JsonUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class JsonFileFormat<T> extends AbstractFileFormat<LongWritable, T, T>
        implements FileFormat<LongWritable, T, T> {

    private final Class<T> clazz;
    private String charset = "UTF-8";

    public JsonFileFormat(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public FunctionEx<InputStream, Stream<T>> mapInputStreamFn() {
        String thisCharset = charset;
        Class<T> thisClazz = clazz;
        return is -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, thisCharset));

            return reader.lines()
                         .map(line -> uncheckCall(() -> JsonUtil.beanFrom(line, thisClazz)))
                         .onClose(() -> uncheckRun(reader::close));
        };
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
                              .loadClass("com.hazelcast.jet.hadoop.impl.JsonInputFormat");
                job.setInputFormatClass(format);
                job.getConfiguration().set("json.bean.class", clazz.getCanonicalName());

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public BiFunctionEx<LongWritable, T, T> projectionFn() {
        return (k, v) -> v;
    }
}
