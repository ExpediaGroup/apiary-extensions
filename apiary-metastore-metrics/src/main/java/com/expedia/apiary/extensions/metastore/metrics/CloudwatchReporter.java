/**
 * Copyright (C) 2018 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expedia.apiary.extensions.metastore.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Gauge;

import java.io.Closeable;
import java.util.TimerTask;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Dimension;

/**
 * TODO: need to update this to implement CodahaleReporter when migrating to hive 3.0.0, 
 * similar to https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/metrics/metrics2/JsonFileMetricsReporter.java
 */

class CloudwatchReporter implements Closeable {
    private Timer timer = null;
    private AmazonCloudWatch cloudWatch = null;
    private MetricRegistry metricRegistry;

    public CloudwatchReporter(MetricRegistry metricRegistry) {
        this.metricRegistry=metricRegistry;
        this.cloudWatch = AmazonCloudWatchClientBuilder.defaultClient();
        this.timer = new Timer(true);
    }

    public void start() {
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
            String namespace = System.getenv("CLOUDWATCH_NAMESPACE");
            Dimension dimension = new Dimension().withName("ecsTaskId") .withValue(System.getenv("ECS_TASK_ID"));
            //all interested metrics are gauges so only writing those to cloudwatch
            for (Map.Entry<String,Gauge> entry : metricRegistry.getGauges().entrySet()) {
                try
                {
                    String metricName = entry.getKey();
                    Double metricValue;
                    if(entry.getValue().getValue() instanceof Long)
                    {
                        metricValue = ((Long)(entry.getValue().getValue())).doubleValue();
                    }
                    else if(entry.getValue().getValue() instanceof Integer)
                    {
                        metricValue = ((Integer)(entry.getValue().getValue())).doubleValue();
                    }
                    else
                    {
                        metricValue = (double)entry.getValue().getValue();
                    }
                    MetricDatum datum = new MetricDatum().withMetricName(metricName).withUnit(StandardUnit.None).withValue(metricValue).withDimensions(dimension);
                    PutMetricDataRequest request = new PutMetricDataRequest().withNamespace(namespace) .withMetricData(datum);
                    PutMetricDataResult response = cloudWatch.putMetricData(request);
                } catch (Exception e) {
                    //ignore threads.deadlocks collection
                }
            }
        }
      }, 0, 60000); //write to cloudwatch every minute
    }

    @Override
    public void close() {
      if (timer != null) {
        this.timer.cancel();
      }
    }
}

