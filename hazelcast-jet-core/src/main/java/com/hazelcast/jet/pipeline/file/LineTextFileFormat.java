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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class LineTextFileFormat extends AbstractFileFormat<String> {

    private final String charset;

    public LineTextFileFormat() {
        this("UTF-8");
    }

    public LineTextFileFormat(String charset) {
        this.charset = charset;
        withOption(INPUT_FORMAT_CLASS, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat");
        withOption(PROJECTION_CLASS, "com.hazelcast.jet.hadoop.impl.ValueToStringProjection");
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
}
