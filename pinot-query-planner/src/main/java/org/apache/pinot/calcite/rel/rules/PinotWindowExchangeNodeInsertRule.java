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
package org.apache.pinot.calcite.rel.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;


/**
 * Special rule for Pinot, this rule is fixed to always insert an exchange or sort exchange below the WINDOW node.
 * TODO:
 *     1. Add support for more than one window group
 *     2. Add support for functions other than:
 *        a. Aggregation functions (AVG, COUNT, MAX, MIN, SUM, BOOL_AND, BOOL_OR)
 *        b. Ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
 */
public class PinotWindowExchangeNodeInsertRule extends RelOptRule {
  public static final PinotWindowExchangeNodeInsertRule INSTANCE =
      new PinotWindowExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  // Supported window functions
  // OTHER_FUNCTION supported are: BOOL_AND, BOOL_OR
  private static final EnumSet<SqlKind> SUPPORTED_WINDOW_FUNCTION_KIND =
      EnumSet.of(SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT, SqlKind.ROW_NUMBER, SqlKind.RANK,
          SqlKind.DENSE_RANK, SqlKind.NTILE, SqlKind.LAG, SqlKind.LEAD, SqlKind.FIRST_VALUE, SqlKind.LAST_VALUE,
          SqlKind.OTHER_FUNCTION);

  public PinotWindowExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(Window.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Window window = call.rel(0);
    return !PinotRuleUtils.isExchange(window.getInput());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Window window = call.rel(0);
    // Perform all validations
    PinotRuleUtils.WindowUtils.validateWindows(window);

    RelNode input = window.getInput();
    Window.Group windowGroup = PinotRuleUtils.WindowUtils.updateLiteralArgumentsInWindowGroup(window);
    Exchange exchange;
    if (windowGroup.keys.isEmpty()) {
      // Empty OVER()
      if (windowGroup.orderKeys.getKeys().isEmpty()) {
        // Add a single Exchange for empty OVER() if sort is not required

        if (PinotRuleUtils.isProject(input)) {
          // Check for empty LogicalProject below LogicalWindow. If present, modify it to be a Literal only project and
          // add a project above.
          Project project = (Project) ((HepRelVertex) input).getCurrentRel();
          if (project.getProjects().isEmpty()) {
            RelNode returnedRelNode = handleEmptyProjectBelowWindow(window, project);
            call.transformTo(returnedRelNode);
            return;
          }
        }

        // TODO: Revisit whether we should use hash distribution
        exchange = PinotLogicalExchange.create(input, RelDistributions.hash(List.of()));
      } else {
        // Only ORDER BY
        // Add a LogicalSortExchange with collation on the order by key(s) and an empty hash partition key
        // TODO: ORDER BY only type queries need to be sorted on both sender and receiver side for better performance.
        //       Sorted input data can use a k-way merge instead of a PriorityQueue for sorting. For now support to
        //       sort on the sender side is not available thus setting this up to only sort on the receiver.
        // TODO: Revisit whether we should use hash distribution
        exchange =
            PinotLogicalSortExchange.create(input, RelDistributions.hash(List.of()), windowGroup.orderKeys, false,
                true);
      }
    } else {
      // All other variants
      // Assess whether this is a PARTITION BY only query or not (includes queries of the type where PARTITION BY and
      // ORDER BY key(s) are the same)
      boolean isPartitionByOnly = isPartitionByOnlyQuery(windowGroup);

      if (isPartitionByOnly) {
        // Only PARTITION BY or PARTITION BY and ORDER BY on the same key(s)
        // Add an Exchange hashed on the partition by keys
        exchange = PinotLogicalExchange.create(input, RelDistributions.hash(windowGroup.keys.toList()));
      } else {
        // PARTITION BY and ORDER BY on different key(s)
        // Add a LogicalSortExchange hashed on the partition by keys and collation based on order by keys
        // TODO: ORDER BY only type queries need to be sorted only on the receiver side unless a hint is set indicating
        //       that the data is already partitioned and sorting can be done on the sender side instead. This way
        //       sorting on the receiver side can be a no-op. Add support for this hint and pass it on. Until sender
        //       side sorting is implemented, setting this hint will throw an error on execution.
        exchange = PinotLogicalSortExchange.create(input, RelDistributions.hash(windowGroup.keys.toList()),
            windowGroup.orderKeys, false, true);
      }
    }
    // NOTE: Need to create a new LogicalWindow to use the modified window group.
    call.transformTo(LogicalWindow.create(window.getTraitSet(), exchange, window.constants, window.getRowType(),
        List.of(windowGroup)));
  }

  private boolean isPartitionByOnlyQuery(Window.Group windowGroup) {
    boolean isPartitionByOnly = false;
    if (windowGroup.orderKeys.getKeys().isEmpty()) {
      return true;
    }

    if (windowGroup.orderKeys.getKeys().size() == windowGroup.keys.asList().size()) {
      Set<Integer> partitionByKeyList = new HashSet<>(windowGroup.keys.toList());
      Set<Integer> orderByKeyList = new HashSet<>(windowGroup.orderKeys.getKeys());
      isPartitionByOnly = partitionByKeyList.equals(orderByKeyList);
    }
    return isPartitionByOnly;
  }

  /**
   * Only empty OVER() type queries using window functions that take no columns as arguments can result in a situation
   * where the LogicalProject below the LogicalWindow is an empty LogicalProject (i.e. no columns are projected).
   * The 'ProjectWindowTransposeRule' looks at the columns present in the LogicalProject above the LogicalWindow and
   * LogicalWindow to decide what to add to the lower LogicalProject when it does the transpose and for such queries
   * if nothing is referenced an empty LogicalProject gets created. Some example queries where this can occur are:
   *
   * SELECT COUNT(*) OVER() from tableName
   * SELECT 42, COUNT(*) OVER() from tableName
   * SELECT ROW_NUMBER() OVER() from tableName
   *
   * This function modifies the empty LogicalProject below the LogicalWindow to add a literal and adds a LogicalProject
   * above LogicalWindow to remove the additional literal column from being projected any further. This also handles
   * the addition of the Exchange under the LogicalWindow.
   *
   * TODO: Explore an option to handle empty LogicalProject by actually projecting empty rows for each entry. This way
   *       there will no longer be a need to add a literal to the empty LogicalProject, but just traverse the number of
   *       rows
   */
  private RelNode handleEmptyProjectBelowWindow(Window window, Project project) {
    RelOptCluster cluster = window.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();

    // Construct the project that goes below the window (which projects a literal)
    final List<RexNode> expsForProjectBelowWindow = Collections.singletonList(
        rexBuilder.makeLiteral(0, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
    final List<String> expsFieldNamesBelowWindow = Collections.singletonList("winLiteral");
    Project projectBelowWindow =
        LogicalProject.create(project.getInput(), project.getHints(), expsForProjectBelowWindow,
            expsFieldNamesBelowWindow);

    // Fix up the inputs to the Window to include the literal column and add an exchange
    final RelDataTypeFactory.Builder outputBuilder = cluster.getTypeFactory().builder();
    outputBuilder.addAll(projectBelowWindow.getRowType().getFieldList());
    outputBuilder.addAll(window.getRowType().getFieldList());

    // This scenario is only possible for empty OVER() which uses functions that have no arguments such as COUNT(*) or
    // ROW_NUMBER(). Add an Exchange with empty hash distribution list
    PinotLogicalExchange exchange =
        PinotLogicalExchange.create(projectBelowWindow, RelDistributions.hash(Collections.emptyList()));
    Window newWindow = new LogicalWindow(window.getCluster(), window.getTraitSet(), exchange, window.getConstants(),
        outputBuilder.build(), window.groups);

    // Create the LogicalProject above window to remove the literal column
    final List<RexNode> expsForProjectAboveWindow = new ArrayList<>();
    final List<String> expsFieldNamesAboveWindow = new ArrayList<>();
    final List<RelDataTypeField> rowTypeWindowInput = newWindow.getRowType().getFieldList();

    int count = 0;
    for (int index = 1; index < rowTypeWindowInput.size(); index++) {
      // Keep only the non-literal fields. We can start from index = 1 since the first and only column from the lower
      // project is the literal column added above.
      final RelDataTypeField relDataTypeField = rowTypeWindowInput.get(index);
      expsForProjectAboveWindow.add(new RexInputRef(index, relDataTypeField.getType()));
      expsFieldNamesAboveWindow.add(String.format("$%d", count));
    }

    return LogicalProject.create(newWindow, project.getHints(), expsForProjectAboveWindow, expsFieldNamesAboveWindow);
  }
}
