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
package org.apache.pinot.query.runtime.operator;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.JoinHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code BaseJoinOperator} implements the basic join algorithm.
 * <p>This algorithm assumes that the right table has to fit in memory since we are not supporting any spilling. It
 * reads the complete right table and materialize the data in memory. Then for each of the left table row, it looks up
 * for the corresponding row(s) from the right table, applies the non-equi evaluators and creates a joint row.
 * <p>For each of the data block received from the left table, it generates a joint data block. The output is in the
 * format of [left_row, right_row].
 */
// TODO: Support memory size based resource limit.
public abstract class BaseJoinOperator extends MultiStageOperator {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseJoinOperator.class);
  protected static final int DEFAULT_MAX_ROWS_IN_JOIN = 1024 * 1024; // 2^20, around 1MM rows
  protected static final JoinOverFlowMode DEFAULT_JOIN_OVERFLOW_MODE = JoinOverFlowMode.THROW;

  protected final MultiStageOperator _leftInput;
  protected final MultiStageOperator _rightInput;
  protected final JoinRelType _joinType;
  protected final DataSchema _resultSchema;
  protected final int _leftColumnSize;
  protected final int _resultColumnSize;
  protected final List<TransformOperand> _nonEquiEvaluators;
  protected final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  // Below are specific parameters to protect the server from a very large join operation.
  // Once the hash table reaches the limit, we will throw exception or break the right table build process.
  // The limit also applies to the number of joined rows in blocks from the left table.
  /**
   * Max rows allowed to build the right table hash collection. Also max rows emitted in each join with a block from
   * the left table.
   */
  protected final int _maxRowsInJoin;
  /**
   * Mode when join overflow happens, supported values: THROW or BREAK.
   *   THROW(default): Break right table build process, and throw exception, no JOIN with left table performed.
   *   BREAK: Break right table build process, continue to perform JOIN operation, results might be partial.
   */
  protected final JoinOverFlowMode _joinOverflowMode;

  protected boolean _isRightTableBuilt;
  @Nullable
  protected MseBlock.Eos _eos;

  public BaseJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    _leftInput = leftInput;
    _rightInput = rightInput;
    _joinType = node.getJoinType();
    _leftColumnSize = leftSchema.size();
    _resultSchema = node.getDataSchema();
    _resultColumnSize = _resultSchema.size();
    List<RexExpression> nonEquiConditions = node.getNonEquiConditions();
    _nonEquiEvaluators = new ArrayList<>(nonEquiConditions.size());
    for (RexExpression nonEquiCondition : nonEquiConditions) {
      _nonEquiEvaluators.add(TransformOperandFactory.getTransformOperand(nonEquiCondition, _resultSchema));
    }
    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInJoin = getMaxRowsInJoin(metadata, nodeHint);
    _joinOverflowMode = getJoinOverflowMode(metadata, nodeHint);
  }

  protected static int getMaxRowsInJoin(Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String maxRowsInJoinStr = joinOptions.get(JoinHintOptions.MAX_ROWS_IN_JOIN);
        if (maxRowsInJoinStr != null) {
          return Integer.parseInt(maxRowsInJoinStr);
        }
      }
    }
    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(opChainMetadata);
    return maxRowsInJoin != null ? maxRowsInJoin : DEFAULT_MAX_ROWS_IN_JOIN;
  }

  protected static JoinOverFlowMode getJoinOverflowMode(Map<String, String> contextMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String joinOverflowModeStr = joinOptions.get(JoinHintOptions.JOIN_OVERFLOW_MODE);
        if (joinOverflowModeStr != null) {
          return JoinOverFlowMode.valueOf(joinOverflowModeStr);
        }
      }
    }
    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(contextMetadata);
    return joinOverflowMode != null ? joinOverflowMode : DEFAULT_JOIN_OVERFLOW_MODE;
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    // TODO: Add separate StatKey for each child join operator.
    return Type.HASH_JOIN;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftInput, _rightInput);
  }

  @Override
  protected MseBlock getNextBlock() {
    if (!_isRightTableBuilt) {
      buildRightTable();
    }
    if (_eos != null) {
      LOGGER.trace("Returning eos");
      return _eos;
    }
    MseBlock mseBlock = buildJoinedDataBlock();
    LOGGER.trace("Returning {} for join operator", mseBlock);
    if (mseBlock.isEos()) {
      _eos = (MseBlock.Eos) mseBlock;
      onEosProduced();
    }
    return mseBlock;
  }

  protected abstract void onEosProduced();

  protected void buildRightTable() {
    LOGGER.trace("Building right table for join operator");
    long startTime = System.currentTimeMillis();
    int numRows = 0;
    MseBlock rightBlock = _rightInput.nextBlock();
    while (rightBlock.isData()) {
      List<Object[]> rows = ((MseBlock.Data) rightBlock).asRowHeap().getRows();
      // Row based overflow check.
      if (rows.size() + numRows > _maxRowsInJoin) {
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          throwForJoinRowLimitExceeded(
              "Cannot build in memory hash table for join operator, reached number of rows limit: " + _maxRowsInJoin);
        } else {
          // Just fill up the buffer.
          int remainingRows = _maxRowsInJoin - numRows;
          rows = rows.subList(0, remainingRows);
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
          // setting only the rightTableOperator to be early terminated and awaits EOS block next.
          _rightInput.earlyTerminate();
        }
      }

      addRowsToRightTable(rows);
      numRows += rows.size();
      sampleAndCheckInterruption();
      rightBlock = _rightInput.nextBlock();
    }

    MseBlock.Eos eosBlock = (MseBlock.Eos) rightBlock;
    if (eosBlock.isError()) {
      _eos = eosBlock;
    } else {
      _isRightTableBuilt = true;
      finishBuildingRightTable();
    }

    _statMap.merge(StatKey.TIME_BUILDING_HASH_TABLE_MS, System.currentTimeMillis() - startTime);
    LOGGER.trace("Finished building right table for join operator");
  }

  protected abstract void addRowsToRightTable(List<Object[]> rows);

  protected abstract void finishBuildingRightTable();

  protected MseBlock buildJoinedDataBlock() {
    LOGGER.trace("Building joined data block for join operator");
    // Keep reading the input blocks until we find a match row or all blocks are processed.
    // TODO: Consider batching the rows to improve performance.
    while (true) {
      if (_eos != null) {
        return _eos;
      }
      LOGGER.trace("Processing next block on left input");
      MseBlock leftBlock = _leftInput.nextBlock();
      if (leftBlock.isEos()) {
        MseBlock.Eos eosBlock = (MseBlock.Eos) leftBlock;
        if (eosBlock.isError()) {
          return eosBlock;
        } else {
          if (needUnmatchedRightRows()) {
            List<Object[]> rows = buildNonMatchRightRows();
            if (!rows.isEmpty()) {
              _eos = SuccessMseBlock.INSTANCE;
              return new RowHeapDataBlock(rows, _resultSchema);
            }
          }
          return leftBlock;
        }
      }
      List<Object[]> rows = buildJoinedRows((MseBlock.Data) leftBlock);
      sampleAndCheckInterruption();
      if (!rows.isEmpty()) {
        return new RowHeapDataBlock(rows, _resultSchema);
      }
    }
  }

  protected abstract List<Object[]> buildJoinedRows(MseBlock.Data leftBlock);

  protected abstract List<Object[]> buildNonMatchRightRows();

  // TODO: Optimize this to avoid unnecessary object copy.
  protected Object[] joinRow(@Nullable Object[] leftRow, @Nullable Object[] rightRow) {
    Object[] resultRow = new Object[_resultColumnSize];
    if (leftRow != null) {
      System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
    }
    if (rightRow != null) {
      System.arraycopy(rightRow, 0, resultRow, _leftColumnSize, rightRow.length);
    }
    return resultRow;
  }

  protected List<Object> joinRowView(@Nullable Object[] leftRow, @Nullable Object[] rightRow) {
    return JoinedRowView.of(leftRow, rightRow, _resultColumnSize, _leftColumnSize);
  }

  protected boolean matchNonEquiConditions(List<Object> row) {
    if (_nonEquiEvaluators.isEmpty()) {
      return true;
    }
    for (TransformOperand evaluator : _nonEquiEvaluators) {
      if (!BooleanUtils.isTrueInternalValue(evaluator.apply(row))) {
        return false;
      }
    }
    return true;
  }

  protected boolean needUnmatchedRightRows() {
    return _joinType == JoinRelType.RIGHT || _joinType == JoinRelType.FULL;
  }

  protected boolean needUnmatchedLeftRows() {
    return _joinType == JoinRelType.LEFT || _joinType == JoinRelType.FULL || _joinType == JoinRelType.LEFT_ASOF;
  }

  protected void earlyTerminateLeftInput() {
    _leftInput.earlyTerminate();
    MseBlock leftBlock = _leftInput.nextBlock();

    while (leftBlock.isData()) {
      leftBlock = _leftInput.nextBlock();
    }
    _eos = (MseBlock.Eos) leftBlock;
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  /**
   * Checks if we have reached the rows limit for joined rows. If the limit has been reached, either an exception is
   * thrown or the left input is early terminated based on the {@link #_joinOverflowMode}.
   *
   * @return {@code true} if the limit has been reached, {@code false} otherwise.
   */
  protected boolean isMaxRowsLimitReached(int numJoinedRows) {
    if (numJoinedRows == _maxRowsInJoin) {
      if (_joinOverflowMode == JoinOverFlowMode.THROW) {
        throwForJoinRowLimitExceeded(
            "Cannot process join, reached number of rows limit: " + _maxRowsInJoin);
      } else {
        // Skip over remaining blocks until we reach the end of stream since we already breached the rows limit.
        logger().info("Terminating join operator early as the maximum number of rows limit was reached: {}",
            _maxRowsInJoin);
        earlyTerminateLeftInput();
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
        return true;
      }
    }

    return false;
  }

  protected static void throwForJoinRowLimitExceeded(String reason) {
    throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
        reason
            + ".\nConsider increasing the limit for the maximum number of rows in a join either via:\n"
            + "  - The query option '" + QueryOptionKey.MAX_ROWS_IN_JOIN + "'\n"
            + "  - The hint '" + JoinHintOptions.MAX_ROWS_IN_JOIN + "' in the '" + PinotHintOptions.JOIN_HINT_OPTIONS
            + "\n"
            + "'Alternatively, if partial results are acceptable, the join overflow mode can be set to '"
            + JoinOverFlowMode.BREAK.name() + "' either via:\n"
            + "  - The query option '" + QueryOptionKey.JOIN_OVERFLOW_MODE + "'\n"
            + "  - The hint '" + JoinHintOptions.JOIN_OVERFLOW_MODE + "' in the '"
            + PinotHintOptions.JOIN_HINT_OPTIONS + "'\n");
  }

  public enum StatKey implements StatMap.Key {
    //@formatter:off
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    MAX_ROWS_IN_JOIN_REACHED(StatMap.Type.BOOLEAN),
    /**
     * How long (CPU time) has been spent on building the hash table.
     */
    TIME_BUILDING_HASH_TABLE_MS(StatMap.Type.LONG);
    //@formatter:on

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }

  /**
   * This util class is a view over the left and right row joined together
   * currently this is used for filtering and input of projection. So if the joined
   * tuple doesn't pass the predicate, the join result is not materialized into Object[].
   *
   * It is debatable whether we always want to use this instead of copying the tuple
   */
  private abstract static class JoinedRowView extends AbstractList<Object> implements List<Object> {
    protected final int _leftSize;
    protected final int _size;

    protected JoinedRowView(int resultColumnSize, int leftSize) {
      _leftSize = leftSize;
      _size = resultColumnSize;
    }

    private static final class BothNotNullView extends JoinedRowView {
      private final Object[] _leftRow;
      private final Object[] _rightRow;

      private BothNotNullView(Object[] leftRow, Object[] rightRow, int resultColumnSize, int leftSize) {
        super(resultColumnSize, leftSize);
        _leftRow = leftRow;
        _rightRow = rightRow;
      }

      @Override
      public Object get(int i) {
        return i < _leftSize ? _leftRow[i] : _rightRow[i - _leftSize];
      }

      @Override
      public Object[] toArray() {
        Object[] resultRow = new Object[_size];
        System.arraycopy(_leftRow, 0, resultRow, 0, _leftSize);
        System.arraycopy(_rightRow, 0, resultRow, _leftSize, _rightRow.length);
        return resultRow;
      }
    }

    private static final class RightNotNullView extends JoinedRowView {
      private final Object[] _rightRow;

      public RightNotNullView(Object[] rightRow, int resultColumnSize, int leftSize) {
        super(resultColumnSize, leftSize);
        _rightRow = rightRow;
      }

      @Override
      public Object get(int i) {
        return i < _leftSize ? null : _rightRow[i - _leftSize];
      }

      @Override
      public Object[] toArray() {
        Object[] resultRow = new Object[_size];
        System.arraycopy(_rightRow, 0, resultRow, _leftSize, _rightRow.length);
        return resultRow;
      }
    }

    private static final class LeftNotNullView extends JoinedRowView {
      private final Object[] _leftRow;

      public LeftNotNullView(Object[] leftRow, int resultColumnSize, int leftSize) {
        super(resultColumnSize, leftSize);
        _leftRow = leftRow;
      }

      @Override
      public Object get(int i) {
        return i < _leftSize ? _leftRow[i] : null;
      }

      @Override
      public Object[] toArray() {
        Object[] resultRow = new Object[_size];
        System.arraycopy(_leftRow, 0, resultRow, 0, _leftSize);
        return resultRow;
      }
    }

    public static JoinedRowView of(@Nullable Object[] leftRow, @Nullable Object[] rightRow, int resultColumnSize,
        int leftSize) {
      if (leftRow == null && rightRow == null) {
        throw new IllegalStateException("both left and right side of join are null");
      }
      if (leftRow == null) {
        return new RightNotNullView(rightRow, resultColumnSize, leftSize);
      }
      if (rightRow == null) {
        return new LeftNotNullView(leftRow, resultColumnSize, leftSize);
      }
      return new BothNotNullView(leftRow, rightRow, resultColumnSize, leftSize);
    }

    @Override
    public int size() {
      return _size;
    }
  }
}
