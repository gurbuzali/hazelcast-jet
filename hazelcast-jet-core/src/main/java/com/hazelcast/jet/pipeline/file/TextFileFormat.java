package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.stream.Stream;

public class TextFileFormat extends AbstractFileFormat<Object, Object, String>
        implements FileFormat<Object, Object, String> {

    private final String charset;

    public TextFileFormat() {
        this("UTF-8");
    }

    public TextFileFormat(String charset) {
        this.charset = charset;
        withOption(INPUT_FORMAT_CLASS, "com.hazelcast.jet.hadoop.impl.WholeTextInputFormat");
    }

    @Override
    public FunctionEx<InputStream, Stream<String>> mapInputStreamFn() {
        String thisCharset = charset;
        return is -> Stream.of(new String(IOUtil.readFully(is), Charset.forName(thisCharset)));
    }

    @Override
    public BiFunctionEx<Object, Object, String> projectionFn() {
        return (k, v) -> v.toString();
    }
}
