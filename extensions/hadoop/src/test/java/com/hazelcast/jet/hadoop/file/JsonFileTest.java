package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.file.JsonFileFormat;
import org.junit.Test;

public class JsonFileTest extends BaseFileTest {

    @Test
    public void shouldReadJsonLinesFile() throws Exception {
        FileSourceBuilder<User> source = FileSources.files("src/test/resources/file.jsonl")
                                                    .withFormat(new JsonFileFormat<>(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }
}
