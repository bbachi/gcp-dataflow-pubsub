package com.bachinalabs;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ReadPubSubMessages extends PTransform<PCollection<PubsubMessage>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<PubsubMessage> input) {

        return input.apply(ParDo.of(new ReadPubSubMessagesFn()));
    }
}
