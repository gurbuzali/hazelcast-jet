package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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
    public FunctionEx<Path, Stream<byte[]>> mapFn() {
        return path -> {

            byte[] bytes = IOUtil.readFully(new FileInputStream(path.toFile()));

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
                              .loadClass("com.hazelcast.jet.hadoop.impl.WholeFileInputFormat");
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
