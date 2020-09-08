package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class LinesTextFileFormat implements FileFormat<Object, Object, String> {

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

    @Override
    public void apply(Object object) {
        if (object instanceof Job) {
            Job job = (Job) object;
            job.setInputFormatClass(TextInputFormat.class);
        }
    }

    @Override
    public BiFunctionEx<Object, Object, String> projectionFn() {
        return (k, v) -> v.toString();
    }
}
