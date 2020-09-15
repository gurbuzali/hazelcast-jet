/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.pipeline.file.AbstractFileFormat.INPUT_FORMAT_CLASS;

/**
 * Hadoop based implementation for FileSourceFactory
 *
 * @param <T> type of the items emitted from the source
 */
public class HadoopSourceFactory<T> implements FileSourceFactory<T> {

    @Override
    public BatchSource<T> create(FileSourceBuilder<T> builder) {

        try {
            Job job = Job.getInstance();

            Configuration configuration = job.getConfiguration();
            applyOptions(configuration, builder.options());

            Map<String, String> formatOptions = builder.format().options();
            applyOptions(configuration, formatOptions);
            String inputFormatClassName = formatOptions.get(INPUT_FORMAT_CLASS);

            Class<? extends InputFormat<?, ?>> inputFormatClass = loadInputFormatClass(inputFormatClassName);
            job.setInputFormatClass(inputFormatClass);

            FileInputFormat.addInputPath(job, new Path(builder.path()));

            return HadoopSources.inputFormat(configuration, builder.format().projectionFn());
        } catch (IOException e) {
            throw new JetException("Could not create a source", e);
        }
    }

    private void applyOptions(Configuration configuration, Map<String, String> options) {
        for (Entry<String, String> entry : options.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
    }

    private Class<? extends InputFormat<?, ?>> loadInputFormatClass(String inputFormatClassName) {

        try {
            @SuppressWarnings("unchecked")
            Class<? extends InputFormat<?, ?>> format = (Class<? extends InputFormat<?, ?>>)
                    Thread.currentThread()
                          .getContextClassLoader()
                          .loadClass(inputFormatClassName);

            return format;

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
