/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.flint;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.AWSEMRServerlessClientBuilder;
import com.amazonaws.services.emrserverless.model.CancelJobRunRequest;
import com.amazonaws.services.emrserverless.model.JobDriver;
import com.amazonaws.services.emrserverless.model.JobRunSummary;
import com.amazonaws.services.emrserverless.model.ListJobRunsRequest;
import com.amazonaws.services.emrserverless.model.SparkSubmit;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;

public class EmrServerlessClient {

  /**
   * Fetch from cluster setting
   */
  private final String applicationId = "xxxxxx";
  private final String executionRoleArn = "xxxxxx";

  /**
   * EMR Serverless client
   */
  private final AWSEMRServerless emrServerless;


  public EmrServerlessClient() {
    this.emrServerless = AWSEMRServerlessClientBuilder.defaultClient();
  }

  public void startJobRun(String jobName, String query) {
    StartJobRunRequest request =
        new StartJobRunRequest()
            .withName(jobName)
            .withApplicationId(applicationId)
            .withExecutionRoleArn(executionRoleArn)
            .withJobDriver(new JobDriver()
                .withSparkSubmit(
                    new SparkSubmit()
                        .withEntryPoint("xxxxxx")
                        .withEntryPointArguments(query)
                        .withSparkSubmitParameters(
                            "--class org.opensearch.flint.SparkFlintApp --conf spark.driver.cores=1 --conf spark.driver.memory=1g " +
                                "--conf spark.executor.cores=2 --conf spark.executor.memory=4g " +
                                "--conf spark.jars=s3://xxxxxx/opensearch-spark-standalone_2.12-0.1.0-20230823.162518-5.jar " +
                                "--conf spark.datasource.flint.host=xxxxxx.us-east-1.es.amazonaws.com " +
                                "--conf spark.datasource.flint.port=-1 --conf spark.datasource.flint.scheme=https " +
                                "--conf spark.datasource.flint.auth=sigv4 --conf spark.datasource.flint.region=us-east-1 " +
                                "--conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/ " +
                                "--conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/ " +
                                "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")));

    emrServerless.startJobRun(request);
  }

  public void cancelJobRun(String jobName) {
    // Find the job with the name
    ListJobRunsRequest request = new ListJobRunsRequest()
        .withApplicationId(applicationId);
    request.putCustomQueryParameter(
        "query", "jobRuns[?domain==`" + jobName + "`].id"); // TODO: doesn't work

    // Should only 1 job match the name
    JobRunSummary jobRun = emrServerless.listJobRuns(request).getJobRuns().get(0);
    System.out.println("Cancelling job: " + jobRun.getId());

    // Cancel the job run
    emrServerless.cancelJobRun(
        new CancelJobRunRequest()
            .withApplicationId(jobRun.getApplicationId())
            .withJobRunId(jobRun.getId()));
  }

  public void submitLivyQuery(String query) {
    // TODO: fetch data source metadata first as above and then submit {@field query} to Livy endpoint
  }
}
