package com.bachinalabs;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

public class ReadPubSubMessagesFn extends DoFn<PubsubMessage, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {

        System.out.println("Payload::::"+c.element().getPayload().toString());
        System.out.println("Bucket Id::::"+ c.element().getAttribute("bucketId"));
        System.out.println("Bucket Id::::"+ c.element().getAttribute("objectId"));
        String bucketName = c.element().getAttribute("bucketId");
        String fileName = c.element().getAttribute("objectId");
        c.output("Bucket Name::" + bucketName +"  File Name::::" + fileName);
    }
}
