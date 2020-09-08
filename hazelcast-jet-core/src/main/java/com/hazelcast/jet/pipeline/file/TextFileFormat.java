package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class TextFileFormat implements FileFormat<Object, Object, String> {

    private Charset utf8;

    public TextFileFormat() {
        utf8 = StandardCharsets.UTF_8;
    }

    public TextFileFormat(Charset utf8) {
        this.utf8 = utf8;
    }

    @Override
    public FunctionEx<? super InputStream, Stream<String>> mapFn() {
        return inputStream -> {

            byte[] bytes = IOUtil.readFully(inputStream);

            return Stream.of(new String(bytes, utf8));
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
                              .loadClass("com.hazelcast.jet.hadoop.impl.WholeTextInputFormat");
                job.setInputFormatClass(format);

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override public BiFunctionEx<Object, Object, String> projectionFn() {
        return (k, v) -> v.toString();
    }
}
