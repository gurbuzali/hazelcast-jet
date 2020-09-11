package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.hadoop.file.generated.SpecificUser;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class AvroFileTest extends BaseFileTest {

    @Test
    public void shouldReadAvroWithSchema() throws Exception {
        createAvroFile();

        FileSourceBuilder<SpecificUser> source = FileSources.files("target/avro/file.avro")
                                                            .withFormat(new AvroFileFormat<>());
        assertItemsInSource(source,
                new SpecificUser("Frantisek", 7),
                new SpecificUser("Ali", 42)
        );

    }

    @Test
    public void shouldReadAvroWithReflection() throws Exception {
        createAvroFile();

        FileSourceBuilder<User> source = FileSources.files("target/avro/file.avro")
                                                    .withFormat(new AvroFileFormat<User>().withReflect(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    private static void createAvroFile() throws IOException {
        Path inputPath = new Path("target/avro");
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(inputPath, true);

        DataFileWriter<SpecificUser> fileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(SpecificUser.class));
        fileWriter.create(SpecificUser.SCHEMA$, fs.create(new Path(inputPath, "file.avro")));
        fileWriter.append(new SpecificUser("Frantisek", 7));
        fileWriter.append(new SpecificUser("Ali", 42));
        fileWriter.close();
        fs.close();
    }
}
