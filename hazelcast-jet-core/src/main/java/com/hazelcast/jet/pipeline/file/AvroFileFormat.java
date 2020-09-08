package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.InputStream;
import java.util.stream.Stream;

public class AvroFileFormat<T> implements FileFormat<AvroKey<T>, NullWritable, T> {

    private boolean reflect;
    private Class<T> reflectClass;

    @Override
    public FunctionEx<? super InputStream, Stream<T>> mapFn() {
        return null;
    }

    @Override
    public void apply(Object object) {
        if (object instanceof Job) {
            Job job = (Job) object;
            job.setInputFormatClass(AvroKeyInputFormat.class);
            if (reflect) {
                Schema schema = ReflectData.get().getSchema(reflectClass);
                AvroJob.setInputKeySchema(job, schema);
            }
        }
    }

    @Override
    public BiFunctionEx<AvroKey<T>, NullWritable, T> projectionFn() {
        return (k, v) -> k.datum();
    }

    public AvroFileFormat<T> withReflect(Class<T> reflectClass) {
        this.reflectClass = reflectClass;
        reflect = true;
        return this;
    }
}
