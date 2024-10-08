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
package org.apache.pinot.queries;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * The <code>ForwardIndexDisabledMultiValueQueriesTest</code> class sets up the index segment for the no forward
 * index multi-value queries test.
 * <p>There are totally 14 columns, 100000 records inside the original Avro file where 10 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, IsMultiValue, FwdIndexDisabled: S1, S2
 *   <li>column1, METRIC, INT, 51594, F, F, F, F, F</li>
 *   <li>column2, METRIC, INT, 42242, F, F, F, F, F</li>
 *   <li>column3, DIMENSION, STRING, 5, F, T, F, F, F</li>
 *   <li>column5, DIMENSION, STRING, 9, F, F, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 18499, F, T, T, T, T</li>
 *   <li>column7, DIMENSION, INT, 359, F, T, T, T, F</li>
 *   <li>column8, DIMENSION, INT, 850, F, T, F, F, F</li>
 *   <li>column9, METRIC, INT, 146, F, T, F, F, F</li>
 *   <li>column10, METRIC, INT, 3960, F, F, F, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 1, T, F, F, F, F</li>
 * </ul>
 */
public class ForwardIndexDisabledMultiValueQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), ForwardIndexDisabledMultiValueQueriesTest.class.getSimpleName());
  private static final String AVRO_DATA = "data" + File.separator + "test_data-mv.avro";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";

  //@formatter:off
  // Hard-coded query filter
  protected static final String FILTER = " WHERE column1 > 100000000"
      + " AND column2 BETWEEN 20000000 AND 1000000000"
      + " AND column3 <> 'w'"
      + " AND (column6 < 500000 OR column7 NOT IN (225, 407))"
      + " AND daysSinceEpoch = 1756015683";
  //@formatter:on

  private IndexSegment _indexSegment;
  // Contains 2 identical index segments.
  private List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    //@formatter:off
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addMetric("column1", DataType.INT)
        .addMetric("column2", DataType.INT)
        .addSingleValueDimension("column3", DataType.STRING)
        .addSingleValueDimension("column5", DataType.STRING)
        .addMultiValueDimension("column6", DataType.INT)
        .addMultiValueDimension("column7", DataType.INT)
        .addSingleValueDimension("column8", DataType.INT)
        .addMetric("column9", DataType.INT)
        .addMetric("column10", DataType.INT)
        .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|DAYS", "1:DAYS")
        .build();

    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig("column6", FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")),
        new FieldConfig("column7", FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName("daysSinceEpoch")
        .setNoDictionaryColumns(List.of("column5"))
        .setInvertedIndexColumns(List.of("column3", "column6", "column7", "column8", "column9"))
        .setCreateInvertedIndexDuringSegmentGeneration(true)
        .setFieldConfigList(fieldConfigs)
        .build();
    //@formatter:on

    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String avroFile = resource.getFile();

    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    generatorConfig.setInputFilePath(avroFile);
    generatorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    generatorConfig.setSegmentName(SEGMENT_NAME);
    generatorConfig.setSkipTimeValueCheck(true);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(generatorConfig);
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME),
        new IndexLoadingConfig(tableConfig, schema));
    Map<String, ColumnMetadata> columnMetadataMap = segment.getSegmentMetadata().getColumnMetadataMap();
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      String column = entry.getKey();
      ColumnMetadata metadata = entry.getValue();
      if (column.equals("column6") || column.equals("column7")) {
        assertTrue(metadata.hasDictionary());
        assertFalse(metadata.isSingleValue());
        assertNull(segment.getForwardIndex(column));
      } else {
        assertNotNull(segment.getForwardIndex(column));
      }
    }

    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Override
  protected String getFilter() {
    return FILTER;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @Test
  public void testSelectStarQueries() {
    // Select * without any filters
    try {
      getBrokerResponse(SELECT_STAR_QUERY);
      Assert.fail("Select * query should fail since forwardIndexDisabled on a select column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("cannot create DataFetcher!"));
    }

    // Select * with filters
    try {
      getBrokerResponse(SELECT_STAR_QUERY + FILTER);
      Assert.fail("Select * query should fail since forwardIndexDisabled on a select column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("scan based filtering not supported!"));
    }
  }

  @Test
  public void testSelectQueries() {
    {
      // Selection query without filters including a column with forwardIndexDisabled enabled on both segments
      String query = "SELECT column1, column5, column6, column9, column10 FROM testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query without filters including a column with forwardIndexDisabled enabled on one segment
      String query = "SELECT column1, column5, column7, column9, column10 FROM testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query with filters including a column with forwardIndexDisabled enabled on both segments
      String query = "SELECT column1, column5, column6, column9, column10 FROM testTable WHERE column6 = 1001";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query with filters including a column with forwardIndexDisabled enabled on one segment
      String query = "SELECT column1, column5, column7, column9, column10 FROM testTable WHERE column7 = 2147483647";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query without filters and without columns with forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_120L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        // Column 1
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Transform function on a selection clause with a forwardIndexDisabled column in transform
      String query = "SELECT ARRAYLENGTH(column6) from testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column in transform");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query with filters (not including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column1 > 100000000"
          + " AND column2 BETWEEN 20000000 AND 1000000000"
          + " AND column3 <> 'w'"
          + " AND daysSinceEpoch = 1756015683 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 62_700L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 62_820);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 304_120L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column6 = 1001 "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 8);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 8L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 32L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals(resultRow[0], 1849044734);
        assertEquals((String) resultRow[1], "AKXcXcIqsqOJFsdwxZ");
        assertEquals(resultRow[2], 674022574);
        assertEquals(resultRow[3], 674022574);
      }
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column7 != 201 "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 399_896L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_016L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column7 IN (201, 2147483647) "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 199_860L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 199_980L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column6 NOT IN "
          + "(1001, 2147483647) ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 174_552L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 174_672L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "EOFxevm");
    }
    {
      // Query with literal only in SELECT
      String query = "SELECT 'marvin' from testTable ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"'marvin'"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 1);
        assertEquals((String) resultRow[0], "marvin");
      }
    }
    {
      // Selection query with '<' filter on a forwardIndexDisabled column without range index available
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column6 < 2147483647 AND "
          + "column6 >= 1001 ORDER BY column1";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a range query column without range index");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("scan based filtering not supported!"));
      }
    }
    {
      // Selection query with '>=' filter on a forwardIndexDisabled column without range index available
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column7 > 201";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a range query column without range index");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("scan based filtering not supported!"));
      }
    }
    {
      // Select query with a filter on a column which doesn't have forwardIndexDisabled enabled
      String query = "SELECT column1, column5, column9 from testTable WHERE column9 < 3890167 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 48L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 128L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 400_000L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5", "column9"},
          new DataSchema.ColumnDataType[]{
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT
          }));
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1Value = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 3);
        assertTrue((Integer) resultRow[0] >= previousColumn1Value);
        previousColumn1Value = (Integer) resultRow[0];
        assertEquals(resultRow[2], 3890166);
      }
    }
    {
      // Transform function on a filter clause for forwardIndexDisabled column in transform
      String query = "SELECT column1, column10 from testTable WHERE ARRAYLENGTH(column6) = 2";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() != null
          && brokerResponseNative.getExceptions().size() > 0);
    }
  }

  @Test
  public void testSelectWithDistinctQueries() {
    // Select a mix of forwardIndexDisabled and non-forwardIndexDisabled columns with distinct
    String query = "SELECT DISTINCT column1, column6, column9 FROM testTable LIMIT 10";
    try {
      getBrokerResponse(query);
      Assert.fail("Query should fail since forwardIndexDisabled on a column in select distinct");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("cannot create DataFetcher!"));
    }
  }

  @Test
  public void testSelectWithGroupByOrderByQueries() {
    {
      // Select a mix of forwardIndexDisabled and non-forwardIndexDisabled columns with group by order by
      String query = "SELECT column1, column6 FROM testTable GROUP BY column1, column6 ORDER BY column1, column6 "
          + " LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Select forwardIndexDisabled columns with group by order by
      String query = "SELECT column7, column6 FROM testTable GROUP BY column7, column6 ORDER BY column7, column6 "
          + " LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Select non-forwardIndexDisabled columns with group by order by
      String query = "SELECT column1, column5 FROM testTable GROUP BY column1, column5 ORDER BY column1, column5 "
          + " LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 800000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      int previousVal = -1;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertTrue((int) resultRow[0] > previousVal);
        previousVal = (int) resultRow[0];
      }
    }
    {
      // Select forwardIndexDisabled columns using transform with group by order by
      String query = "SELECT ARRAYLENGTH(column6) FROM testTable GROUP BY ARRAYLENGTH(column6) ORDER BY "
          + "ARRAYLENGTH(column6) LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Test a select with a VALUEIN transform function with group by
      String query = "SELECT VALUEIN(column6, '1001') from testTable WHERE column6 IN (1001) GROUP BY "
          + "VALUEIN(column6, '1001') LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
  }

  @Test
  public void testSelectWithAggregationQueries() {
    {
      // Allowed aggregation functions on forwardIndexDisabled columns
      String query = "SELECT maxmv(column7), minmv(column6), minmaxrangemv(column6), distinctcountmv(column7), "
          + "distinctcounthllmv(column6), distinctcountrawhllmv(column7) from testTable";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"maxmv(column7)", "minmv(column6)",
          "minmaxrangemv(column6)", "distinctcountmv(column7)", "distinctcounthllmv(column6)",
          "distinctcountrawhllmv(column7)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
              DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG,
              DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 6);
        assertEquals(resultRow[0], 2.147483647E9);
        assertEquals(resultRow[1], 1001.0);
        assertEquals(resultRow[2], 2.147482646E9);
        assertEquals(resultRow[3], 359);
        assertEquals(resultRow[4], 20039L);
      }
    }
    {
      // Not allowed aggregation functions on forwardIndexDisabled columns
      String query = "SELECT summv(column7), avgmv(column6) from testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with group by on non-forwardIndexDisabled
      // column
      String query = "SELECT column1, maxmv(column6) from testTable GROUP BY column1";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with group by order by on
      // non-forwardIndexDisabled column
      String query = "SELECT column1, maxmv(column6) from testTable GROUP BY column1 ORDER BY column1";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with group by on non-forwardIndexDisabled
      // column but order by on allowed aggregation function on forwardIndexDisabled column
      String query = "SELECT column1, max(column9) from testTable GROUP BY column1 ORDER BY minmv(column6)";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with a filter - results in trying to scan which
      // fails
      String query = "SELECT maxmv(column7), minmv(column6) from testTable WHERE column7 = 201";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with a filter - results in trying to scan which
      // fails
      String query = "SELECT max(column1), minmv(column6) from testTable WHERE column1 > 15935";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
      // column
      String query = "SELECT max(column1), sum(column9) from testTable WHERE column7 = 2147483647";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 199_756L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_512L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column1)", "sum(column9)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertEquals(resultRow[0], 2.147313491E9);
        assertEquals(resultRow[1], 1.38051889779548E14);
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
      // column and group by order by on non-forwardIndexDisabled column
      String query = "SELECT column1, max(column1), sum(column9) from testTable WHERE column7 = 2147483647 GROUP BY "
          + "column1 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getExceptions() == null
          || brokerResponseNative.getExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 199_756L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_512L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getExceptions());
      assertEquals(brokerResponseNative.getExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "max(column1)", "sum(column9)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE,
              DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      int previousVal = -1;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 3);
        assertTrue((int) resultRow[0] > previousVal);
        previousVal = (int) resultRow[0];
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
      // column and group by on non-forwardIndexDisabled column order by on forwardIndexDisabled aggregation column
      String query = "SELECT column1, max(column1), sum(column9) from testTable WHERE column7 = 201 GROUP BY "
          + "column1 ORDER BY minmv(column6)";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column
      String query = "SELECT MAX(ARRAYLENGTH(column6)) from testTable LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column with group by
      String query = "SELECT column1, MAX(ARRAYLENGTH(column6)) from testTable GROUP BY column1 LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column with group by order by
      String query = "SELECT column1, MAX(ARRAYLENGTH(column6)) from testTable GROUP BY column1 ORDER BY column1 "
          + "DESC LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
  }
}
