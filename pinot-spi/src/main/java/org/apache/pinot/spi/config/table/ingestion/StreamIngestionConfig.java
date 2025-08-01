/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.DisasterRecoveryMode;


/**
 * Contains all the configs related to the streams for ingestion
 */
public class StreamIngestionConfig extends BaseJsonConfig {

  @JsonPropertyDescription("All configs for the streams from which to ingest")
  private final List<Map<String, String>> _streamConfigMaps;

  @JsonPropertyDescription("Whether to use column major mode when creating the segment.")
  private boolean _columnMajorSegmentBuilderEnabled = true;

  @JsonPropertyDescription("Whether to track offsets of the filtered stream messages during consumption.")
  private boolean _trackFilteredMessageOffsets;

  @JsonPropertyDescription("Whether pauseless consumption is enabled for the table")
  private boolean _pauselessConsumptionEnabled;

  @JsonPropertyDescription("Enforce consumption of segments in order of segment creation by the controller")
  private boolean _enforceConsumptionInOrder;

  @JsonPropertyDescription("If enabled, Server always relies on ideal state to get previous segment. If disabled, "
      + "server uses sequence id - 1 for previous segment")
  private boolean _useIdealStateToCalculatePreviousSegment;

  @JsonPropertyDescription("Policy to determine the behaviour of parallel consumption.")
  private ParallelSegmentConsumptionPolicy _parallelSegmentConsumptionPolicy;

  @JsonPropertyDescription("Recovery mode which is used to decide how to recover a segment online in IS but having no"
      + " completed (immutable) replica on any server in pause-less ingestion")
  private DisasterRecoveryMode _disasterRecoveryMode = DisasterRecoveryMode.DEFAULT;

  @JsonCreator
  public StreamIngestionConfig(@JsonProperty("streamConfigMaps") List<Map<String, String>> streamConfigMaps) {
    _streamConfigMaps = streamConfigMaps;
  }

  public List<Map<String, String>> getStreamConfigMaps() {
    return _streamConfigMaps;
  }

  public void setColumnMajorSegmentBuilderEnabled(boolean enableColumnMajorSegmentCreation) {
    _columnMajorSegmentBuilderEnabled = enableColumnMajorSegmentCreation;
  }

  public boolean getColumnMajorSegmentBuilderEnabled() {
    return _columnMajorSegmentBuilderEnabled;
  }

  public void setTrackFilteredMessageOffsets(boolean trackFilteredMessageOffsets) {
    _trackFilteredMessageOffsets = trackFilteredMessageOffsets;
  }

  public boolean isTrackFilteredMessageOffsets() {
    return _trackFilteredMessageOffsets;
  }

  public boolean isPauselessConsumptionEnabled() {
    return _pauselessConsumptionEnabled;
  }

  public void setPauselessConsumptionEnabled(boolean pauselessConsumptionEnabled) {
    _pauselessConsumptionEnabled = pauselessConsumptionEnabled;
  }

  public boolean isEnforceConsumptionInOrder() {
    return _enforceConsumptionInOrder;
  }

  public void setEnforceConsumptionInOrder(boolean enforceConsumptionInOrder) {
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
  }

  public boolean isUseIdealStateToCalculatePreviousSegment() {
    return _useIdealStateToCalculatePreviousSegment;
  }

  public void setUseIdealStateToCalculatePreviousSegment(boolean useIdealStateToCalculatePreviousSegment) {
    _useIdealStateToCalculatePreviousSegment = useIdealStateToCalculatePreviousSegment;
  }

  @Nullable
  public ParallelSegmentConsumptionPolicy getParallelSegmentConsumptionPolicy() {
    return _parallelSegmentConsumptionPolicy;
  }

  public void setParallelSegmentConsumptionPolicy(ParallelSegmentConsumptionPolicy parallelSegmentConsumptionPolicy) {
    _parallelSegmentConsumptionPolicy = parallelSegmentConsumptionPolicy;
  }

  public DisasterRecoveryMode getDisasterRecoveryMode() {
    return _disasterRecoveryMode;
  }

  public void setDisasterRecoveryMode(DisasterRecoveryMode disasterRecoveryMode) {
    _disasterRecoveryMode = disasterRecoveryMode;
  }
}
