/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.util;

import com.google.common.base.Strings;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * HoodieRealtimeFileSplit specific implementation of CustomSplitConverter.
 * Extracts customSplitInfo from HoodieRealtimeFileSplit and reconstructs HoodieRealtimeFileSplit from Map.
 */
public class HudiRealtimeSplitConverter
        implements CustomSplitConverter
{
    private static final String CUSTOM_SPLIT_CLASS_KEY = "custom_split_class";
    private static final String HUDI_DELTA_FILEPATHS_KEY = "hudi_delta_filepaths";
    private static final String HUDI_BASEPATH_KEY = "hudi_basepath";
    private static final String HUDI_MAX_COMMIT_TIME_KEY = "hudi_max_commit_time";

    @Override
    public Optional<Map<String, String>> extractAnyCustomSplitInfo(FileSplit split)
    {
        if (split instanceof HoodieRealtimeFileSplit) {
            Map<String, String> customSplitInfo = new HashMap<>();
            HoodieRealtimeFileSplit hudiSplit = (HoodieRealtimeFileSplit) split;
            customSplitInfo.put(CUSTOM_SPLIT_CLASS_KEY, HoodieRealtimeFileSplit.class.getName());
            customSplitInfo.put(HUDI_DELTA_FILEPATHS_KEY, String.join(",", hudiSplit.getDeltaLogPaths()));
            customSplitInfo.put(HUDI_BASEPATH_KEY, hudiSplit.getBasePath());
            customSplitInfo.put(HUDI_MAX_COMMIT_TIME_KEY, hudiSplit.getMaxCommitTime());
            return Optional.of(customSplitInfo);
        }
        return Optional.empty();
    }

    @Override
    public Optional<FileSplit> recreateSplitWithCustomInfo(FileSplit split, Map<String, String> customSplitInfo) throws IOException
    {
        requireNonNull(customSplitInfo);
        if (customSplitInfo.containsKey(CUSTOM_SPLIT_CLASS_KEY) && customSplitInfo.get(CUSTOM_SPLIT_CLASS_KEY).equals(HoodieRealtimeFileSplit.class.getName())) {
            List<String> deltaLogPaths = Strings.isNullOrEmpty(customSplitInfo.get(HUDI_DELTA_FILEPATHS_KEY)) ? Collections.emptyList() :
                    Arrays.asList(customSplitInfo.get(HUDI_DELTA_FILEPATHS_KEY).split(","));
            split = new HoodieRealtimeFileSplit(split, customSplitInfo.get(HUDI_BASEPATH_KEY), deltaLogPaths, customSplitInfo.get(HUDI_MAX_COMMIT_TIME_KEY));
            return Optional.of(split);
        }
        return Optional.empty();
    }
}
