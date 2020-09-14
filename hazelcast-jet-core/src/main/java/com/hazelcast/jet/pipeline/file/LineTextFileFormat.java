package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class LineTextFileFormat extends AbstractFileFormat<Object, Object, String> implements FileFormat<Object,
        Object, String> {


    private String charset;

    public LineTextFileFormat() {
        charset = "UTF-8";
    }

    public LineTextFileFormat(String charset) {
        this.charset = charset;
    }

    @Override
    public FunctionEx<InputStream, Stream<String>> mapInputStreamFn() {
        String thisCharset = charset;
        return is -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, thisCharset));
            return reader.lines()
                         .onClose(() -> uncheckRun(reader::close));
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
