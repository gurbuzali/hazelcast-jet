package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class RawBytesFileFormat implements FileFormat<NullWritable, BytesWritable, byte[]> {

    private Charset utf8;

    public RawBytesFileFormat() {
        utf8 = StandardCharsets.UTF_8;
    }

    public RawBytesFileFormat(Charset utf8) {
        this.utf8 = utf8;
    }

    @Override
    public FunctionEx<? super InputStream, Stream<byte[]>> mapFn() {
        return inputStream -> {

            byte[] bytes = IOUtil.readFully(inputStream);

            return Stream.of(bytes);
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
                              .loadClass("com.hazelcast.jet.hadoop.format.WholeFileInputFormat");
                job.setInputFormatClass(format);

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override public BiFunctionEx<NullWritable, BytesWritable, byte[]> projectionFn() {
        return (k, v) -> v.copyBytes();
    }
}
