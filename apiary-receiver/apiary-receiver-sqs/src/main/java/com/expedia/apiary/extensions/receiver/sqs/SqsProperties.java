/**
 * Copyright (C) 2018-2019 Expedia Inc.
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
package com.expedia.apiary.extensions.receiver.sqs;

import com.amazonaws.regions.Regions;

public class SqsProperties {

  private String queue;
  private Regions region;
  private Integer waitTimeSeconds;
  private Integer maxMessages;
  private String awsAccessKey;
  private String awsSecretKey;

  private SqsProperties(String queue, Regions region, Integer waitTimeSeconds, Integer maxMessages, String awsAccessKey,
                       String awsSecretKey) {
    this.queue = queue;
    this.region = region;
    this.waitTimeSeconds = waitTimeSeconds;
    this.maxMessages = maxMessages;
    this.awsAccessKey = awsAccessKey;
    this.awsSecretKey = awsSecretKey;
  }

  public String getQueue() {
    return queue;
  }

  public Regions getRegion() {
    return region;
  }

  public Integer getWaitTimeSeconds() {
    return waitTimeSeconds;
  }

  public String getAwsAccessKey() {
    return awsAccessKey;
  }

  public String getAwsSecretKey() {
    return awsSecretKey;
  }

  public Integer getMaxMessages() {
    return maxMessages;
  }

  public static final class Builder {

    private String queue;
    private Regions region;
    private Integer waitTimeSeconds;
    private Integer maxMessages;
    private String awsAccessKey;
    private String awsSecretKey;

    public Builder(String queue, String awsAccessKey, String awsSecretKey) {
      this.queue = queue;
      this.awsAccessKey = awsAccessKey;
      this.awsSecretKey = awsSecretKey;
    }

    public Builder withRegion(Regions region) {
      this.region = region;
      return this;
    }

    public Builder withWaitTimeSeconds(Integer waitTimeSeconds) {
      this.waitTimeSeconds = waitTimeSeconds;
      return this;
    }

    public Builder withMaxMessages(Integer maxMessages) {
      this.maxMessages = maxMessages;
      return this;
    }

    public SqsProperties build() {
      return new SqsProperties(queue, region, waitTimeSeconds, maxMessages, awsAccessKey, awsSecretKey);
    }
  }
}
