package com.quipper.beam.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.quipper.beam.etl.options.MongodbToGcsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.apache.beam.sdk.io.TextIO;
import org.bson.json.JsonWriterSettings;
import com.mongodb.client.model.Filters;
import java.util.*;





public class MongodbToGcs {
    public static final Logger LOG = LoggerFactory.getLogger(MongodbToGcs.class);
    public static String convertDocumentToString(Document document){
        return document.toJson(JsonWriterSettings
        .builder()
        .build());
    }
   
    public static void main(String[] args) throws Exception {
        MongodbToGcsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MongodbToGcsOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        

       PCollection<String> mongoDatas = pipeline.apply(
            MongoDbIO.read()
            .withUri("mongodb://"+options.getSourceDbUser()+":"+options.getSourceDbPassword()+"@"+options.getSourceDbHost()+":"+options.getSourceDbPort()+"/?replicaSet=rs0&readPreference=secondary")
            .withDatabase(options.getSourceDbName())
            .withCollection(options.getSourceCollection())
            .withQueryFn(null)
    
        ).apply(
            "convert bson.document to string",ParDo.of(new DoFn<Document,String>() {
                @ProcessElement
                public void processElement(@Element Document document, OutputReceiver<String> out) {
                    out.output(
                        document.toJson(JsonWriterSettings
                            .builder()
                            .build()).replace("$oid", "oid")
                    );
                }
            })
        );

        UUID uuid = UUID.randomUUID();
        
        mongoDatas.apply("Write to GCS", TextIO.write()
                .to("gs://"+options.getGcsBucketDestination()+"/data/"+options.getSourceDbName()+"/"+options.getSourceCollection()+"/" + uuid)
                .withSuffix(".jsonl"));

        pipeline.run();
    }

}
