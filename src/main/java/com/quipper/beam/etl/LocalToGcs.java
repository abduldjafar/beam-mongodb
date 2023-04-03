package com.quipper.beam.etl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.quipper.beam.etl.options.LocalToGcsOption;
import com.quipper.beam.etl.utils.GeneralUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.io.TextIO;
import java.util.*;
import java.util.stream.Collectors;
import com.google.gson.Gson;


public class LocalToGcs {
    public static final Logger LOG = LoggerFactory.getLogger(LocalToGcs.class);
    private static final GeneralUtils UTILS = new GeneralUtils();


    //ParDo for Row (SQL) -> String
    public static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().getValues()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            c.output(line);
        }
    }

    public static class JsonObjectToRow extends DoFn<String, Row> {
        private Schema schema;

        public JsonObjectToRow(Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(@Element String jsonString, OutputReceiver<Row> out) {
            Gson gson = new Gson();
                    Map<?, ?> map = gson.fromJson(jsonString, Map.class);

                    List<Object> rowsDatas = new ArrayList<Object>();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
   
                        if (entry.getValue() == null) {
                            continue;
                        }

                        rowsDatas.add(entry.getValue().toString());
                    }

                   
                    Row row = Row.withSchema(this.schema)
                                .addValues(rowsDatas)
                                .build();
                    
                    out.output(row);
        }
    }

    public static void main(String[] args) throws Exception {

        LocalToGcsOption options = PipelineOptionsFactory.fromArgs(args).withValidation().as(LocalToGcsOption.class);
        Schema dataSchema = UTILS.generateSchema(options.getSchemaFilename());


        Pipeline pipeline = Pipeline.create(options);
        

       PCollection<String> jsonDatas = pipeline.apply(
            TextIO.read().from(options.getLocalFilename())
        );

        PCollection<String> finalResult = jsonDatas.apply(
            "generate row",ParDo.of(new JsonObjectToRow(dataSchema))
        ).setRowSchema(dataSchema).apply("transform_sql", SqlTransform.query(
            "SELECT * FROM PCOLLECTION")
        ).apply("transform_to_string", ParDo.of(new RowToString()));

        UUID uuid = UUID.randomUUID();


        finalResult.apply("write_to_local", 
                TextIO.write()
                .to("data/"+uuid)
                .withoutSharding()
                .withSuffix(".csv"));
        pipeline.run();
    }
}
