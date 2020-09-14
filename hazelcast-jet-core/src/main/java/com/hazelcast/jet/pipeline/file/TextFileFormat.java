package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.stream.Stream;

public class TextFileFormat extends AbstractFileFormat<Object, Object, String>
        implements FileFormat<Object, Object, String> {

    private String charset;

    public TextFileFormat() {
        charset = "UTF-8";
    }

    public TextFileFormat(String charset) {
        this.charset = charset;
    }

    @Override
    public FunctionEx<InputStream, Stream<String>> mapInputStreamFn() {
        String thisCharset = charset;
        return is -> {
            byte[] bytes = IOUtil.readFully(is);
            return Stream.of(new String(bytes, Charset.forName(thisCharset)));
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

    @Override
    public BiFunctionEx<Object, Object, String> projectionFn() {
        return (k, v) -> v.toString();
    }
}
