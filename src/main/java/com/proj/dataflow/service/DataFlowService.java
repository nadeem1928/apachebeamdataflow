package com.proj.dataflow.service;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DataFlowService {

	@Value("${inputFilePath}")
    private String inputFilePath;
    
    public void triggerDataflowLocal() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

     // Read data from a file present in local
        pipeline
            .apply(TextIO.read().from("C:\\Users\\Sumanth\\file.txt"))
            .apply(MapElements
                .into(TypeDescriptor.of(String.class))
                .via((String line) -> {
                    System.out.println(line);
                    return line;
                }));

        pipeline.run().waitUntilFinish();
    }
    
    public void triggerDataflowFromBucket() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

     // Read data from a file present in bucket
        pipeline
            .apply(TextIO.read().from("gs://your-bucket-name/path/to/your/object"))
            .apply(MapElements
                .into(TypeDescriptor.of(String.class))
                .via((String line) -> {
                    System.out.println(line);
                    return line;
                }));

        pipeline.run().waitUntilFinish();
    }
}
