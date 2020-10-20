package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.ModifiedLongStreamSourceP;

import java.util.Arrays;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JobPriorities {

    private static final String RESULT_MAP_NAME = "results-by-job";
    private static final int DURATION_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        args = new String[]{"1", "10000"};

        JetInstance jet = Jet.bootstrappedInstance();
        new JobPrioritiesGui(jet.getMap(RESULT_MAP_NAME));

        try {
            Job[] jobs = new Job[args.length];

            for (int i = 0; i < args.length; i++) {
                long priority = Long.parseLong(args[i]);
                String jobName = priority + ":job" + i;
                jobs[i] = jet.newJob(buildPipeline(jobName), new JobConfig().setName(jobName));
            }
            SECONDS.sleep(DURATION_SECONDS);

            Arrays.stream(jobs).forEach(job -> {
                job.cancel();
                job.join();
            });
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline(String jobName) {
        Pipeline p = Pipeline.create();
//        p.readFrom(TestSources.itemStream(2_000))
//                .withIngestionTimestamps()
//                .rollingAggregate(AggregateOperations.counting())
//                .map(count -> entry(jobName, count))
//                .writeTo(Sinks.map(RESULT_MAP_NAME));

        p.readFrom(modifiedLongStream())
                .withNativeTimestamps(0).setLocalParallelism(2)
                .groupingKey(item -> 0L)
                .window(WindowDefinition.tumbling(2000))
                .aggregate(AggregateOperations.counting())
                .peek()
                .writeTo(Sinks.mapWithMerging(RESULT_MAP_NAME, result -> jobName, KeyedWindowResult::getValue, Long::sum));
        return p;
    }

    public static StreamSource<Long> modifiedLongStream() {
        return Sources.streamFromProcessorWithWatermarks("longStream",
                true,
                eventTimePolicy -> ProcessorMetaSupplier.of(() -> new ModifiedLongStreamSourceP(eventTimePolicy))
        );
    }

}
