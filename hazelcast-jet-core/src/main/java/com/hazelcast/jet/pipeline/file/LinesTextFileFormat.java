package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.FunctionEx;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class LinesTextFileFormat implements FileFormat<String> {

    private Charset charset;

    public LinesTextFileFormat() {
        charset = StandardCharsets.UTF_8;
    }

    public LinesTextFileFormat(Charset charset) {
        this.charset = charset;
    }

    @Override
    public FunctionEx<? super InputStream, Stream<String>> mapFn() {
        return responseInputStream -> {

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(responseInputStream, charset));

            return reader.lines();
        };
    }
}
