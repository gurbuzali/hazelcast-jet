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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.json.JsonUtil;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class JsonFileFormat<T> extends AbstractFileFormat<T> {

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

    public JsonFileFormat<T> withCharset(String charset) {
        this.charset = charset;
        return this;
    }
}
