package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.IOUtil;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class TextFileFormat implements FileFormat<String> {

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
}
