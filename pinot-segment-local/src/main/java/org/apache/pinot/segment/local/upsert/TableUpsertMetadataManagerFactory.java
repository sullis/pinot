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
package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableUpsertMetadataManagerFactory {
  private TableUpsertMetadataManagerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableUpsertMetadataManagerFactory.class);

  public static TableUpsertMetadataManager create(PinotConfiguration instanceUpsertConfig, TableConfig tableConfig,
      Schema schema, TableDataManager tableDataManager, SegmentOperationsThrottler segmentOperationsThrottler) {
    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkArgument(tableConfig.isUpsertEnabled(), "Upsert must be enabled for table: %s",
        tableNameWithType);
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    assert upsertConfig != null;

    TableUpsertMetadataManager metadataManager;
    String metadataManagerClass = upsertConfig.getMetadataManagerClass();
    if (metadataManagerClass == null) {
      metadataManagerClass = instanceUpsertConfig.getProperty(Upsert.DEFAULT_METADATA_MANAGER_CLASS);
    }
    if (StringUtils.isNotBlank(metadataManagerClass)) {
      LOGGER.info("Creating TableUpsertMetadataManager with class: {} for table: {}", metadataManagerClass,
          tableNameWithType);
      try {
        metadataManager =
            (TableUpsertMetadataManager) Class.forName(metadataManagerClass).getConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Caught exception while constructing TableUpsertMetadataManager with class: %s for table: %s",
                metadataManagerClass, tableNameWithType), e);
      }
    } else {
      LOGGER.info("Creating ConcurrentMapTableUpsertMetadataManager for table: {}", tableNameWithType);
      metadataManager = new ConcurrentMapTableUpsertMetadataManager();
    }
    metadataManager.init(instanceUpsertConfig, tableConfig, schema, tableDataManager, segmentOperationsThrottler);
    return metadataManager;
  }
}
