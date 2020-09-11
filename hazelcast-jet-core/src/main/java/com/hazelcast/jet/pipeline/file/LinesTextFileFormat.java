package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.stream.Stream;

public class LinesTextFileFormat implements FileFormat<Object, Object, String> {


    private String charset;

    public LinesTextFileFormat() {
        charset = "UTF-8";
    }

    public LinesTextFileFormat(String charset) {
        this.charset = charset;
    }

    @Override
    public FunctionEx<Path, Stream<String>> mapFn() {
        String thisCharset = charset;
        return path -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile()), thisCharset));
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
