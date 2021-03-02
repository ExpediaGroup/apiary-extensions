/**
 * Copyright (C) 2018-2021 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.hooks.pathconversion.converters;

import java.util.regex.Matcher;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import lombok.extern.slf4j.Slf4j;

import com.expediagroup.apiary.extensions.hooks.pathconversion.config.Configuration;
import com.expediagroup.apiary.extensions.hooks.pathconversion.models.PathConversion;

@Slf4j
public class PathConverter extends GenericConverter {

    public PathConverter(Configuration configuration) {
        super(configuration);
    }

    public boolean convertPath(StorageDescriptor sd) {
        boolean changedScheme = false;
        for (PathConversion pathConversion : getConfiguration().getPathConversions()) {
            Matcher matcher = pathConversion.pathPattern.matcher(sd.getLocation());
            if (matcher.find()) {
                for (Integer captureGroup : pathConversion.captureGroups) {
                    if (hasCaptureGroup(matcher, captureGroup, sd.getLocation())) {
                        String newLocation = sd.getLocation().replace(matcher.group(captureGroup), pathConversion.replacementValue);
                        log.info("Switching storage location {} to {}.", sd.getLocation(), newLocation);
                        sd.setLocation(newLocation);
                        changedScheme = true;
                    }
                }
            }
        }

        return changedScheme;
    }

    private boolean hasCaptureGroup(Matcher matcher, int groupNumber, String location) {
        try {
            matcher.group(groupNumber);
            return true;
        } catch (IndexOutOfBoundsException ex) {
            log.warn("No capture group number {} found on location[{}]", groupNumber, location);
            return false;
        }
    }
}
