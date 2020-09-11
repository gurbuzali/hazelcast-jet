package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.CsvFileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

public class CsvFileTest extends BaseFileTest {

    @Test
    public void shouldReadCsvFile() throws Exception {

        FileSourceBuilder<User> source = FileSources.files("src/test/resources/file.csv")
                                                    .withFormat(new CsvFileFormat<>(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }
}
