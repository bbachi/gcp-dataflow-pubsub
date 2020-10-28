package com.bachinalabs;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class StreamingPipepline {

    public static void main( String[] args ) {

        // Start by defining the options for the pipeline.
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        // For Cloud execution, set the Cloud Platform project, staging location,
        // and specify DataflowRunner.
        options.setProject("staticweb-test");
        options.setRegion("us-central1");
        options.setStagingLocation("gs://bbachi_dataflow_temp/binaries/");
        options.setGcpTempLocation("gs://bbachi_dataflow_temp/temp/");
        options.setNetwork("default");
        options.setSubnetwork("regions/us-central1/subnetworks/default");
        options.setRunner(DataflowRunner.class);

        // Then create the pipeline.
        Pipeline p = Pipeline.create(options);

        p.apply("Read PubSub Messages", PubsubIO.readMessagesWithAttributesAndMessageId().fromTopic("projects/staticweb-test/topics/TOPIC_DATAFLOW_FILES"))
                .apply(new ReadPubSubMessages())
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Write Bucket Names", TextIO.write().to("gs://bbachi_dataflow_output/")
                       .withWindowedWrites().withNumShards(1));


        p.run().waitUntilFinish();

    }
}
