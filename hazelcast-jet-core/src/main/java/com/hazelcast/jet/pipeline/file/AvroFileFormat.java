/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.NullWritable;

import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * FileFormat for avro files
 *
 *
 * @param <T>
 */
public class AvroFileFormat<T> extends AbstractFileFormat<AvroKey<T>, NullWritable, T>
        implements FileFormat<AvroKey<T>, NullWritable, T> {

    // Taken from org.apache.avro.mapreduce.AvroJob.CONF_INPUT_KEY_SCHEMA, which is private
    private static final String CONF_INPUT_KEY_SCHEMA = "avro.schema.input.key";

    private Class<T> reflectClass;

    public AvroFileFormat() {
        withOption(INPUT_FORMAT_CLASS, AvroKeyInputFormat.class.getCanonicalName());
    }

    @Override
    public FunctionEx<Path, Stream<T>> localMapFn() {
        DatumReader<T> datumReader = datumReader();
        return (path) -> {
            DataFileReader<T> reader = new DataFileReader<>(path.toFile(), datumReader);
            return StreamSupport.stream(reader.spliterator(), false)
                    .onClose(() -> uncheckRun(reader::close));
        };
    }

    @Override
    public BiFunctionEx<AvroKey<T>, NullWritable, T> projectionFn() {
        return (k, v) -> k.datum();
    }

    public AvroFileFormat<T> withReflect(Class<T> reflectClass) {
        this.reflectClass = reflectClass;
        Schema schema = ReflectData.get().getSchema(reflectClass);
        withOption(CONF_INPUT_KEY_SCHEMA, schema.toString());
        return this;
    }

    private DatumReader<T> datumReader() {
        return reflectClass == null ? new SpecificDatumReader<>() : new ReflectDatumReader<>(reflectClass);
    }
}
