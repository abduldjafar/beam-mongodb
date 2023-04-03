package com.quipper.beam.etl.utils;

import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

import org.apache.beam.sdk.schemas.Schema;


public class GeneralUtils {
  public Schema generateSchema(String fileName){

    // create a reader
    Schema.Builder outputSchema = Schema.builder();

    Gson gson = new Gson();

        try (Reader reader = new FileReader(fileName)) {

            Map<?, ?> map = gson.fromJson(reader, Map.class);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                

                System.out.println(entry.getKey());

                switch (entry.getValue().toString()) {
                    case "string":
                        outputSchema = outputSchema.addStringField(entry.getKey().toString());
                        break;
                    case "boolean":
                        outputSchema = outputSchema.addBooleanField(entry.getKey().toString());
                    case "timestamp":
                        outputSchema = outputSchema.addDateTimeField(entry.getKey().toString());

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
       
        return  outputSchema.build();
  }
}
