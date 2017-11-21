package com.samsung.pay.emr.hadoop.wordcount;

import com.amazonaws.auth.*;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class Main {

    public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials("AWS_ACCESS_KEY", "AWS_SECRET_KEY");
        AmazonElasticMapReduce client = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("ap-northeast-2")
                .build();

        // A custom step
        HadoopJarStepConfig config = new HadoopJarStepConfig()
                .withJar("s3://<bucket_name>/<jar_path>") // have to set schema 's3://'
                .withMainClass("<main_class>") // optional main class, this can be omitted if jar above has a manifest
                .withArgs("arg0", "arg1", "arg2..."); // optional list of arguments
        StepConfig customStep = new StepConfig("<job_name>", config);

        AddJobFlowStepsResult result = client.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId("<cluster_id>")
                .withSteps(customStep));

        System.out.println(result.getStepIds());
    }

}
