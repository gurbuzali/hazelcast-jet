package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.hadoop.file.generated.SpecificUser;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.test.Assertions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroFileTest extends JetTestSupport {

    @Test
    public void shouldReadAvroWithSchema() throws Exception {
        createAvroFile();

        BatchSource<SpecificUser> source = FileSources.files("target/avro/file.avro")
                                                      .useHadoopForLocalFiles()
                                                      .withFormat(new AvroFileFormat<SpecificUser>())
                                                      .build();

        Pipeline p = Pipeline.create();

        p.readFrom(source)
         .apply(Assertions.assertCollected(collected -> {
             assertThat(collected).containsOnly(
                     new SpecificUser("Frantisek", 7),
                     new SpecificUser("Ali", 42)
             );

         }));


        JetInstance jet = createJetMember();
        jet.newJob(p).join();
    }

    @Test
    public void shouldReadAvroWithReflection() throws Exception {
        createAvroFile();

        BatchSource<User> source = FileSources.files("target/avro/file.avro")
                                              .useHadoopForLocalFiles()
                                              .withFormat(new AvroFileFormat<User>().withReflect(User.class))
                                              .build();

        Pipeline p = Pipeline.create();

        p.readFrom(source)
         .apply(Assertions.assertCollected(collected -> {
             assertThat(collected).containsOnly(
                     new User("Frantisek", 7),
                     new User("Ali", 42)
             );
         }));


        JetInstance jet = createJetMember();
        jet.newJob(p).join();
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
