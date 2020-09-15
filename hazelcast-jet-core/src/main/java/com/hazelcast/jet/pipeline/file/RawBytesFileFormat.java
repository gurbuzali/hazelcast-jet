package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.InputStream;
import java.util.stream.Stream;

public class RawBytesFileFormat extends AbstractFileFormat<NullWritable, BytesWritable, byte[]>
        implements FileFormat<NullWritable, BytesWritable, byte[]> {

    public RawBytesFileFormat() {
        withOption(INPUT_FORMAT_CLASS, "com.hazelcast.jet.hadoop.impl.WholeFileInputFormat");
    }

    @Override
    public FunctionEx<InputStream, Stream<byte[]>> mapInputStreamFn() {
        return is -> Stream.of(IOUtil.readFully(is));
    }

    @Override
    public BiFunctionEx<NullWritable, BytesWritable, byte[]> projectionFn() {
        return (k, v) -> v.copyBytes();
    }
}
