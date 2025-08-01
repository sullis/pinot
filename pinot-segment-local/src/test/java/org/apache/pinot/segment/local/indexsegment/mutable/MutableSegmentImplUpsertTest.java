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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MutableSegmentImplUpsertTest {
  private static final String SCHEMA_FILE_PATH = "data/test_upsert_schema.json";
  private static final String DATA_FILE_PATH = "data/test_upsert_data.json";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "secondsSinceEpoch";
  private static final String OTHER_COMPARISON_COLUMN = "otherComparisonColumn";

  private MutableSegmentImpl _mutableSegmentImpl;
  private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;

  private UpsertConfig createPartialUpsertConfig(HashFunction hashFunction) {
    UpsertConfig upsertConfigWithHash = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithHash.setHashFunction(hashFunction);
    upsertConfigWithHash.setComparisonColumns(List.of(TIME_COLUMN, OTHER_COMPARISON_COLUMN));
    return upsertConfigWithHash;
  }

  private UpsertConfig createFullUpsertConfig(HashFunction hashFunction) {
    UpsertConfig upsertConfigWithHash = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithHash.setHashFunction(hashFunction);
    return upsertConfigWithHash;
  }

  private void setup(UpsertConfig upsertConfigWithHash)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfigWithHash)
        .setNullHandlingEnabled(true)
        .build();
    URL schemaResourceUrl = getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    assertNotNull(schemaResourceUrl);
    Schema schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    TransformPipeline transformPipeline = new TransformPipeline(tableConfig, schema);
    URL dataResourceUrl = getClass().getClassLoader().getResource(DATA_FILE_PATH);
    assertNotNull(dataResourceUrl);
    File jsonFile = new File(dataResourceUrl.getFile());
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    _partitionUpsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    _mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COLUMN, _partitionUpsertMetadataManager,
            null);

    GenericRow reuse = new GenericRow();
    try (RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.JSON, jsonFile,
        schema.getColumnNames(), null)) {
      while (recordReader.hasNext()) {
        recordReader.next(reuse);
        TransformPipeline.Result result = transformPipeline.processRow(reuse);
        for (GenericRow transformedRow : result.getTransformedRows()) {
          _mutableSegmentImpl.index(transformedRow, null);
        }
        reuse.clear();
      }
    }
  }

  private void tearDown()
      throws IOException {
    if (_mutableSegmentImpl != null) {
      _mutableSegmentImpl.destroy();
      _mutableSegmentImpl = null;
    }
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.stop();
      _partitionUpsertMetadataManager.close();
      _partitionUpsertMetadataManager = null;
    }
  }

  @Test
  public void testHashFunctions()
      throws Exception {
    testUpsertIngestion(createFullUpsertConfig(HashFunction.NONE));
    testUpsertIngestion(createFullUpsertConfig(HashFunction.MD5));
    testUpsertIngestion(createFullUpsertConfig(HashFunction.MURMUR3));
  }

  @Test
  public void testMultipleComparisonColumns()
      throws Exception {
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.NONE));
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.MD5));
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.MURMUR3));
  }

  private void testUpsertIngestion(UpsertConfig upsertConfig)
      throws Exception {
    setup(upsertConfig);
    try {
      ImmutableRoaringBitmap bitmap = _mutableSegmentImpl.getValidDocIds().getMutableRoaringBitmap();
      if (upsertConfig.getComparisonColumns() == null) {
        // aa
        assertFalse(bitmap.contains(0));
        assertTrue(bitmap.contains(1));
        assertFalse(bitmap.contains(2));
        assertFalse(bitmap.contains(3));
        // bb
        assertFalse(bitmap.contains(4));
        assertTrue(bitmap.contains(5));
        assertFalse(bitmap.contains(6));
      } else {
        // aa
        assertFalse(bitmap.contains(0));
        assertFalse(bitmap.contains(1));
        assertTrue(bitmap.contains(2));
        assertFalse(bitmap.contains(3));
        // Confirm that both comparison column values have made it into the persisted upserted doc
        assertEquals(_mutableSegmentImpl.getValue(2, TIME_COLUMN), 1567205397L);
        assertEquals(_mutableSegmentImpl.getValue(2, OTHER_COMPARISON_COLUMN), 1567205395L);
        assertFalse(_mutableSegmentImpl.getDataSource(TIME_COLUMN).getNullValueVector().isNull(2));
        assertFalse(_mutableSegmentImpl.getDataSource(OTHER_COMPARISON_COLUMN).getNullValueVector().isNull(2));

        // bb
        assertFalse(bitmap.contains(4));
        assertTrue(bitmap.contains(5));
        assertFalse(bitmap.contains(6));
        // Confirm that comparison column values have made it into the persisted upserted doc
        assertEquals(_mutableSegmentImpl.getValue(5, TIME_COLUMN), 1567205396L);
        assertEquals(_mutableSegmentImpl.getValue(5, OTHER_COMPARISON_COLUMN), Long.MIN_VALUE);
        assertFalse(_mutableSegmentImpl.getDataSource(TIME_COLUMN).getNullValueVector().isNull(5));
        assertTrue(_mutableSegmentImpl.getDataSource(OTHER_COMPARISON_COLUMN).getNullValueVector().isNull(5));
      }
    } finally {
      tearDown();
    }
  }
}
