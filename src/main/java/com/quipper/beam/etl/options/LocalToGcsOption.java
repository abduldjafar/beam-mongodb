package com.quipper.beam.etl.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.*;

public interface LocalToGcsOption extends PipelineOptions, SdkHarnessOptions, DataflowPipelineOptions {

    @Description("GCP bucket destination")
    @Validation.Required
    String getGcsBucketDestination();

    void setGcsBucketDestination(String value);


    @Description("local file name")
    @Validation.Required
    String getLocalFilename();

    void setLocalFilename(String value);


    @Description("schema file name")
    @Validation.Required
    String getSchemaFilename();

    void setSchemaFilename(String value);

  
}
