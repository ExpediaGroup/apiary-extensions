/**
 * Copyright (C) 2018-2026 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.gluesync.listener.metrics;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.jmx.ObjectNameFactory;

/**
 * Decodes tag key-value pairs encoded as {@code name[k=v,k=v]} by
 * {@link MetricService#taggedNameMapper()} into proper JMX ObjectName key properties,
 * so jmx-exporter can reference them as Prometheus labels without regex string-splitting.
 *
 * <p>Un-tagged metrics (no {@code [}) fall back to the standard
 * {@code domain:name=<name>,type=<type>} ObjectName.
 */
class TaggedObjectNameFactory implements ObjectNameFactory {

  private static final Logger log = LoggerFactory.getLogger(TaggedObjectNameFactory.class);

  @Override
  public ObjectName createName(String type, String domain, String name) {
    int bracketIdx = name.indexOf('[');
    try {
      if (bracketIdx < 0) {
        return new ObjectName(domain + ":name=" + name + ",type=" + type);
      }
      String baseName = name.substring(0, bracketIdx);
      String tagStr = name.substring(bracketIdx + 1, name.length() - 1);
      StringBuilder sb = new StringBuilder(domain).append(":name=").append(baseName);
      for (String kv : tagStr.split(",")) {
        sb.append(',').append(kv);
      }
      sb.append(",type=").append(type);
      return new ObjectName(sb.toString());
    } catch (MalformedObjectNameException e) {
      log.warn("Could not create JMX ObjectName for metric '{}', falling back to quoted name", name, e);
      try {
        return new ObjectName(domain + ":name=" + ObjectName.quote(name) + ",type=" + type);
      } catch (MalformedObjectNameException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
