package com.hazelcast.jet.hadoop;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Map.Entry;

public class HadoopSourceFactory<T> implements FileSourceFactory<T> {

    @Override
    public BatchSource<T> create(FileSourceBuilder<T> builder) {

        try {
            Job job = Job.getInstance();

            Configuration configuration = job.getConfiguration();
            for (Entry<String, String> entry : builder.options().entrySet()) {
                configuration.set(entry.getKey(), entry.getValue());
            }
            builder.format().apply(job);

            FileInputFormat.addInputPath(job, new Path(builder.path()));

            return HadoopSources.inputFormat(configuration, builder.format().projectionFn());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
