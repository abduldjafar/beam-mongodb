package com.quipper.beam.etl.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.*;

public interface MongodbToGcsOptions extends PipelineOptions, SdkHarnessOptions, DataflowPipelineOptions {

     // Source database options
     @Description("Source db name")
     @Validation.Required
     String getSourceDbName();
 
     void setSourceDbName(String value);


     @Hidden
     @Description("Source db username")
     @Validation.Required
     String getSourceDbUser();
 
     void setSourceDbUser(String value);
 
     @Hidden
     @Description("Source db password")
     @Validation.Required
     String getSourceDbPassword();
 
     void setSourceDbPassword(String value);

    @Description("Source db host")
    @Validation.Required
    String getSourceDbHost();

    void setSourceDbHost(String value);

    @Description("Source db collection")
    @Validation.Required
    String getSourceCollection();

    void setSourceCollection(String value);
    
    @Description("GCP bucket destination")
    @Validation.Required
    String getGcsBucketDestination();

    void setGcsBucketDestination(String value);


    @Description("GCP bucket destination")
    @Validation.Required
    String getSourceDbPort();

    void setSourceDbPort(String value);
    
  
}
