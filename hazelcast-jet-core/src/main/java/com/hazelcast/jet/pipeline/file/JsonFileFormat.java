package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.json.JsonUtil;
import org.apache.hadoop.io.LongWritable;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class JsonFileFormat<T> extends AbstractFileFormat<LongWritable, T, T>
        implements FileFormat<LongWritable, T, T> {

    public static final String JSON_INPUT_FORMAT_BEAN_CLASS = "json.bean.class";

    private final Class<T> clazz;
    private String charset = "UTF-8";

    public JsonFileFormat(Class<T> clazz) {
        this.clazz = clazz;
        withOption(INPUT_FORMAT_CLASS, "com.hazelcast.jet.hadoop.impl.JsonInputFormat");
        withOption(JSON_INPUT_FORMAT_BEAN_CLASS, clazz.getCanonicalName());
    }

    @Override
    public FunctionEx<InputStream, Stream<T>> mapInputStreamFn() {
        String thisCharset = charset;
        Class<T> thisClazz = clazz;
        return is -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, thisCharset));

            return reader.lines()
                         .map(line -> uncheckCall(() -> JsonUtil.beanFrom(line, thisClazz)))
                         .onClose(() -> uncheckRun(reader::close));
        };
    }

    @Override
    public BiFunctionEx<LongWritable, T, T> projectionFn() {
        return (k, v) -> v;
    }

    public JsonFileFormat<T> withCharset(String charset) {
        this.charset = charset;
        return this;
    }
}
