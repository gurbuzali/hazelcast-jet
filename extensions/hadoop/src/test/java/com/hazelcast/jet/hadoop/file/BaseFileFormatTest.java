package com.hazelcast.jet.hadoop.file;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.test.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(Parameterized.class)
public abstract class BaseFileFormatTest extends JetTestSupport {

    @Parameter
    public boolean useHadoop;

    @Parameters(name = "{index}: useHadoop={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true, false);
    }

    protected <T> void assertItemsInSource(FileSourceBuilder<T> source, T... items) {
        assertItemsInSource(source, collected -> assertThat(collected).containsOnly(items));
    }

    protected <T> void assertItemsInSource(
            FileSourceBuilder<T> source, ConsumerEx<List<T>> assertion
    ) {
        if (useHadoop) {
            source.useHadoopForLocalFiles();
        }

        Pipeline p = Pipeline.create();

        p.readFrom(source.build())
         .apply(Assertions.assertCollected(assertion));

        JetInstance jet = createJetMember();
        jet.newJob(p).join();
    }
}
