package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.json.JsonUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class JsonFileFormat<T> implements FileFormat<LongWritable, T, T> {

    private final Class<T> clazz;
    private String charset = "UTF-8";

    public JsonFileFormat(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public FunctionEx<Path, Stream<T>> mapFn() {
        String thisCharset = charset;
        Class<T> thisClazz = clazz;
        return path -> {
            // TODO close this correctly
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile()), thisCharset));
            return reader.lines().map(line -> {
                try {
                    return JsonUtil.beanFrom(line, thisClazz);
                } catch (IOException e) {
                    throw sneakyThrow(e);
                }
            });
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
