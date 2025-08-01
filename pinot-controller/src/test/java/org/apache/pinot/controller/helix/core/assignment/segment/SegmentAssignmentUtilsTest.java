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
package org.apache.pinot.controller.helix.core.assignment.segment;

import it.unimi.dsi.fastutil.ints.IntIntPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentAssignmentUtilsTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final String INSTANCE_NAME_PREFIX = "instance_";

  @Test
  public void testRebalanceTableWithHelixAutoRebalanceStrategy() {
    int numSegments = 100;
    List<String> segments = SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, numSegments);
    int numInstances = 10;
    List<String> instances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, numInstances);

    // Uniformly spray segments to the instances (i0 represents instance_0, s0 represents segment_0)
    // [    i0,    i1,    i2,    i3,    i4,    i5,    i6,    i7,    i8,    i9]
    //  s0(r0) s0(r1) s0(r2) s1(r0) s1(r1) s1(r2) s2(r0) s2(r1) s2(r2) s3(r0)
    //  s3(r1) s3(r2) s4(r0) s4(r1) s4(r2) s5(r0) s5(r1) s5(r2) s6(r0) s6(r1)
    //  ...
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    int assignedInstanceId = 0;
    for (String segmentName : segments) {
      List<String> instancesAssigned = new ArrayList<>(NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        instancesAssigned.add(instances.get(assignedInstanceId));
        assignedInstanceId = (assignedInstanceId + 1) % numInstances;
      }
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, instances);
    int[] expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    int numSegmentsPerInstance = numSegments * NUM_REPLICAS / numInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(SegmentAssignmentUtils.rebalanceNonReplicaGroupBasedTable(currentAssignment, instances, NUM_REPLICAS),
        currentAssignment);

    // Replace instance_0 with instance_10
    List<String> newInstances = new ArrayList<>(instances);
    String newInstanceName = INSTANCE_NAME_PREFIX + 10;
    newInstances.set(0, newInstanceName);
    Map<String, Map<String, String>> newAssignment =
        SegmentAssignmentUtils.rebalanceNonReplicaGroupBasedTable(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // All segments on instance_0 should be moved to instance_10
    Map<String, IntIntPair> numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), 2);
    assertEquals(numSegmentsToMovePerInstance.get(newInstanceName), IntIntPair.of(30, 0));
    String oldInstanceName = INSTANCE_NAME_PREFIX + 0;
    assertEquals(numSegmentsToMovePerInstance.get(oldInstanceName), IntIntPair.of(0, 30));
    for (String segmentName : segments) {
      if (currentAssignment.get(segmentName).containsKey(oldInstanceName)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newInstanceName));
      }
    }

    // Remove 5 instances
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4]
    // }
    int newNumInstances = numInstances - 5;
    newInstances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    newAssignment =
        SegmentAssignmentUtils.rebalanceNonReplicaGroupBasedTable(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // The segments are not perfectly balanced, but should be deterministic
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    assertEquals(numSegmentsAssignedPerInstance[0], 60);
    assertEquals(numSegmentsAssignedPerInstance[1], 60);
    assertEquals(numSegmentsAssignedPerInstance[2], 60);
    assertEquals(numSegmentsAssignedPerInstance[3], 60);
    assertEquals(numSegmentsAssignedPerInstance[4], 60);
    numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), numInstances);
    assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + 0), IntIntPair.of(30, 0));
    assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + 1), IntIntPair.of(30, 0));
    assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + 2), IntIntPair.of(30, 0));
    assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + 3), IntIntPair.of(30, 0));
    assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + 4), IntIntPair.of(30, 0));
    for (int i = 5; i < 10; i++) {
      assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + i), IntIntPair.of(0, 30));
    }

    // Add 5 instances (i0 represents instance_0)
    // {
    //   0_0=[i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14]
    // }
    newNumInstances = numInstances + 5;
    newInstances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    newAssignment =
        SegmentAssignmentUtils.rebalanceNonReplicaGroupBasedTable(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 20 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[newNumInstances];
    int newNumSegmentsPerInstance = numSegments * NUM_REPLICAS / newNumInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, newNumSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each new added instance should have 20 segments to be moved to it
    numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), newNumInstances);
    for (int i = 0; i < numInstances; i++) {
      assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + i), IntIntPair.of(0, 10));
    }
    for (int i = numInstances; i < newNumInstances; i++) {
      assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + i), IntIntPair.of(20, 0));
    }

    // Change all instances
    // {
    //   0_0=[i_0, i_1, i_2, i_3, i_4, i_5, i_6, i_7, i_8, i_9]
    // }
    String newInstanceNamePrefix = "i_";
    newInstances = SegmentAssignmentTestUtils.getNameList(newInstanceNamePrefix, numInstances);
    newAssignment =
        SegmentAssignmentUtils.rebalanceNonReplicaGroupBasedTable(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each instance should have 30 segments to be moved to it
    numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), 2 * numInstances);
    for (int i = 0; i < numInstances; i++) {
      assertEquals(numSegmentsToMovePerInstance.get(newInstanceNamePrefix + i), IntIntPair.of(30, 0));
      assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + i), IntIntPair.of(0, 30));
    }
  }

  @Test
  public void testRebalanceReplicaGroupBasedTable() {
    // Table is rebalanced on a per partition basis, so testing rebalancing one partition is enough

    int numSegments = 90;
    List<String> segments = SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, numSegments);
    Map<Integer, List<String>> partitionIdToSegmentsMap = Collections.singletonMap(0, segments);
    int numInstances = 9;
    List<String> instances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, numInstances);

    // {
    //   0_0=[instance_0, instance_1, instance_2],
    //   0_1=[instance_3, instance_4, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    InstancePartitions instancePartitions = new InstancePartitions(null);
    int numInstancesPerReplicaGroup = numInstances / NUM_REPLICAS;
    int instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> instancesForReplicaGroup = new ArrayList<>(numInstancesPerReplicaGroup);
      for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
        instancesForReplicaGroup.add(instances.get(instanceIdToAdd++));
      }
      instancePartitions.setInstances(0, replicaGroupId, instancesForReplicaGroup);
    }

    // Uniformly spray segments to the instances:
    // Replica-group 0: [instance_0, instance_1, instance_2],
    // Replica-group 1: [instance_3, instance_4, instance_5],
    // Replica-group 2: [instance_6, instance_7, instance_8]
    //                   segment_0   segment_1   segment_2
    //                   segment_3   segment_4   segment_5
    //                   ...
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < numSegments; segmentId++) {
      List<String> instancesAssigned = new ArrayList<>(NUM_REPLICAS);
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int assignedInstanceId = segmentId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        instancesAssigned.add(instances.get(assignedInstanceId));
      }
      currentAssignment.put(segments.get(segmentId),
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // There should be 90 segments assigned
    assertEquals(currentAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, instances);
    int[] expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    int numSegmentsPerInstance = numSegments * NUM_REPLICAS / numInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(SegmentAssignmentUtils.rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions,
        partitionIdToSegmentsMap), currentAssignment);

    // Replace instance_0 with instance_9, instance_4 with instance_10
    // {
    //   0_0=[instance_9, instance_1, instance_2],
    //   0_1=[instance_3, instance_10, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    List<String> newInstances = new ArrayList<>(numInstances);
    List<String> newReplicaGroup0Instances = new ArrayList<>(instancePartitions.getInstances(0, 0));
    String newReplicaGroup0Instance = INSTANCE_NAME_PREFIX + 9;
    newReplicaGroup0Instances.set(0, newReplicaGroup0Instance);
    newInstances.addAll(newReplicaGroup0Instances);
    List<String> newReplicaGroup1Instances = new ArrayList<>(instancePartitions.getInstances(0, 1));
    String newReplicaGroup1Instance = INSTANCE_NAME_PREFIX + 10;
    newReplicaGroup1Instances.set(1, newReplicaGroup1Instance);
    newInstances.addAll(newReplicaGroup1Instances);
    List<String> newReplicaGroup2Instances = instancePartitions.getInstances(0, 2);
    newInstances.addAll(newReplicaGroup2Instances);
    InstancePartitions newInstancePartitions = new InstancePartitions(null);
    newInstancePartitions.setInstances(0, 0, newReplicaGroup0Instances);
    newInstancePartitions.setInstances(0, 1, newReplicaGroup1Instances);
    newInstancePartitions.setInstances(0, 2, newReplicaGroup2Instances);
    Map<String, Map<String, String>> newAssignment =
        SegmentAssignmentUtils.rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions,
            partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // All segments on instance_0 should be moved to instance_9, all segments on instance_4 should be moved to
    // instance_10
    Map<String, IntIntPair> numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), 4);
    assertEquals(numSegmentsToMovePerInstance.get(newReplicaGroup0Instance), IntIntPair.of(30, 0));
    assertEquals(numSegmentsToMovePerInstance.get(newReplicaGroup1Instance), IntIntPair.of(30, 0));
    String oldReplicaGroup0Instance = INSTANCE_NAME_PREFIX + 0;
    String oldReplicaGroup1Instance = INSTANCE_NAME_PREFIX + 4;
    assertEquals(numSegmentsToMovePerInstance.get(oldReplicaGroup0Instance), IntIntPair.of(0, 30));
    assertEquals(numSegmentsToMovePerInstance.get(oldReplicaGroup0Instance), IntIntPair.of(0, 30));
    for (String segmentName : segments) {
      Map<String, String> oldInstanceStateMap = currentAssignment.get(segmentName);
      if (oldInstanceStateMap.containsKey(oldReplicaGroup0Instance)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newReplicaGroup0Instance));
      }
      if (oldInstanceStateMap.containsKey(oldReplicaGroup1Instance)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newReplicaGroup1Instance));
      }
    }

    // Remove 3 instances (1 from each replica-group)
    // {
    //   0_0=[instance_0, instance_1],
    //   0_1=[instance_3, instance_4],
    //   0_2=[instance_6, instance_7]
    // }
    int newNumInstances = numInstances - 3;
    int newNumInstancesPerReplicaGroup = newNumInstances / NUM_REPLICAS;
    newInstances = new ArrayList<>(newNumInstances);
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> newInstancesForReplicaGroup =
          instancePartitions.getInstances(0, replicaGroupId).subList(0, newNumInstancesPerReplicaGroup);
      newInstancePartitions.setInstances(0, replicaGroupId, newInstancesForReplicaGroup);
      newInstances.addAll(newInstancesForReplicaGroup);
    }
    newAssignment = SegmentAssignmentUtils.rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions,
        partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 45 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[newNumInstances];
    int newNumSegmentsPerInstance = numSegments * NUM_REPLICAS / newNumInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, newNumSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each instance should have 15 segments to be moved to it
    numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), numInstances);
    for (String instanceName : instances) {
      if (newInstances.contains(instanceName)) {
        assertEquals(numSegmentsToMovePerInstance.get(instanceName), IntIntPair.of(15, 0));
      } else {
        assertEquals(numSegmentsToMovePerInstance.get(instanceName), IntIntPair.of(0, 30));
      }
    }

    // Add 6 instances (2 to each replica-group)
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_9, instance_10],
    //   0_1=[instance_3, instance_4, instance_5, instance_11, instance_12],
    //   0_2=[instance_6, instance_7, instance_8, instance_13, instance_14]
    // }
    newNumInstances = numInstances + 6;
    newNumInstancesPerReplicaGroup = newNumInstances / NUM_REPLICAS;
    newInstances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    instanceIdToAdd = numInstances;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> newInstancesForReplicaGroup = new ArrayList<>(instancePartitions.getInstances(0, replicaGroupId));
      for (int i = 0; i < newNumInstancesPerReplicaGroup - numInstancesPerReplicaGroup; i++) {
        newInstancesForReplicaGroup.add(newInstances.get(instanceIdToAdd++));
      }
      newInstancePartitions.setInstances(0, replicaGroupId, newInstancesForReplicaGroup);
    }
    newAssignment = SegmentAssignmentUtils.rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions,
        partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 18 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[newNumInstances];
    newNumSegmentsPerInstance = numSegments * NUM_REPLICAS / newNumInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, newNumSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each new added instance should have 18 segments to be moved to it
    numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), newNumInstances);
    for (int i = 0; i < numInstances; i++) {
      assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + i), IntIntPair.of(0, 12));
    }
    for (int i = numInstances; i < newNumInstances; i++) {
      assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + i), IntIntPair.of(18, 0));
    }

    // Change all instances
    // {
    //   0_0=[i_0, i_1, i_2],
    //   0_1=[i_3, i_4, i_5],
    //   0_2=[i_6, i_7, i_8]
    // }
    String newInstanceNamePrefix = "i_";
    newInstances = SegmentAssignmentTestUtils.getNameList(newInstanceNamePrefix, numInstances);
    instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> instancesForReplicaGroup = new ArrayList<>(numInstancesPerReplicaGroup);
      for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
        instancesForReplicaGroup.add(newInstances.get(instanceIdToAdd++));
      }
      newInstancePartitions.setInstances(0, replicaGroupId, instancesForReplicaGroup);
    }
    newAssignment = SegmentAssignmentUtils.rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions,
        partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each instance should have 30 segments to be moved to it
    numSegmentsToMovePerInstance =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToMovePerInstance.size(), 2 * numInstances);
    for (int i = 0; i < numInstances; i++) {
      assertEquals(numSegmentsToMovePerInstance.get(newInstanceNamePrefix + i), IntIntPair.of(30, 0));
      assertEquals(numSegmentsToMovePerInstance.get(INSTANCE_NAME_PREFIX + i), IntIntPair.of(0, 30));
    }
  }

  @Test
  public void testCompletedConsumingOfflineSegmentAssignment() {
    // Test case 1: No committing segments
    Map<String, Map<String, String>> segmentAssignment = new TreeMap<>();

    // Add completed segments (at least one instance ONLINE)
    segmentAssignment.put("online_and_online", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("instance_1", "instance_2"), SegmentStateModel.ONLINE));
    segmentAssignment.put("online_only", SegmentAssignmentUtils.getInstanceStateMap(
        List.of("instance_1"), SegmentStateModel.ONLINE));

    // Add consuming segments (at least one instance CONSUMING, no ONLINE)
    segmentAssignment.put("consuming_and_consuming", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("instance_1", "instance_2"), SegmentStateModel.CONSUMING));
    segmentAssignment.put("consuming_only", SegmentAssignmentUtils.getInstanceStateMap(
        List.of("instance_3"), SegmentStateModel.CONSUMING));

    // Add offline segments (all instances OFFLINE)
    segmentAssignment.put("offline_and_offline", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("instance_1", "instance_2"), SegmentStateModel.OFFLINE));
    segmentAssignment.put("offline_only", SegmentAssignmentUtils.getInstanceStateMap(
        List.of("instance_4"), SegmentStateModel.OFFLINE));

    // Add mixed state segments (should be classified as completed if any ONLINE)
    Map<String, String> mixedStateMap1 = new HashMap<>();
    mixedStateMap1.put("instance_1", SegmentStateModel.ONLINE);
    mixedStateMap1.put("instance_2", SegmentStateModel.CONSUMING);
    segmentAssignment.put("online_and_consuming", mixedStateMap1);

    Map<String, String> mixedStateMap2 = new HashMap<>();
    mixedStateMap2.put("instance_1", SegmentStateModel.ONLINE);
    mixedStateMap2.put("instance_2", SegmentStateModel.OFFLINE);
    segmentAssignment.put("online_and_offline", mixedStateMap2);

    Map<String, String> mixedStateMap3 = new HashMap<>();
    mixedStateMap3.put("instance_1", SegmentStateModel.ONLINE);
    mixedStateMap3.put("instance_2", SegmentStateModel.CONSUMING);
    mixedStateMap3.put("instance_3", SegmentStateModel.OFFLINE);
    segmentAssignment.put("online_consuming_offline", mixedStateMap3);

    Map<String, String> mixedStateMap4 = new HashMap<>();
    mixedStateMap4.put("instance_1", SegmentStateModel.CONSUMING);
    mixedStateMap4.put("instance_2", SegmentStateModel.OFFLINE);
    segmentAssignment.put("consuming_and_offline", mixedStateMap4);

    // Test without committing segments
    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment assignment1 =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(segmentAssignment, null);

    assertEquals(assignment1.getCompletedSegmentAssignment().size(), 5);
    assertTrue(assignment1.getCompletedSegmentAssignment().containsKey("online_and_online"));
    assertTrue(assignment1.getCompletedSegmentAssignment().containsKey("online_only"));
    assertTrue(assignment1.getCompletedSegmentAssignment().containsKey("online_and_consuming"));
    assertTrue(assignment1.getCompletedSegmentAssignment().containsKey("online_and_offline"));
    assertTrue(assignment1.getCompletedSegmentAssignment().containsKey("online_consuming_offline"));

    assertEquals(assignment1.getConsumingSegmentAssignment().size(), 3);
    assertTrue(assignment1.getConsumingSegmentAssignment().containsKey("consuming_and_consuming"));
    assertTrue(assignment1.getConsumingSegmentAssignment().containsKey("consuming_and_offline"));
    assertTrue(assignment1.getConsumingSegmentAssignment().containsKey("consuming_only"));

    assertEquals(assignment1.getOfflineSegmentAssignment().size(), 2);
    assertTrue(assignment1.getOfflineSegmentAssignment().containsKey("offline_and_offline"));
    assertTrue(assignment1.getOfflineSegmentAssignment().containsKey("offline_only"));

    // Test case 2: With committing segments
    Set<String> committingSegments = new HashSet<>(Arrays.asList("online_and_online", "online_and_consuming"));

    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment assignment2 =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(segmentAssignment, committingSegments);

    assertEquals(assignment2.getCompletedSegmentAssignment().size(), 3);
    assertTrue(assignment2.getCompletedSegmentAssignment().containsKey("online_only"));
    assertTrue(assignment2.getCompletedSegmentAssignment().containsKey("online_and_offline"));
    assertTrue(assignment2.getCompletedSegmentAssignment().containsKey("online_consuming_offline"));

    assertEquals(assignment2.getConsumingSegmentAssignment().size(), 5);
    assertTrue(assignment2.getConsumingSegmentAssignment().containsKey("online_and_online")); // ONLINE but COMMITTING
    assertTrue(
        assignment2.getConsumingSegmentAssignment().containsKey("online_and_consuming")); // ONLINE but COMMITTING
    assertTrue(assignment2.getConsumingSegmentAssignment().containsKey("consuming_and_consuming"));
    assertTrue(assignment2.getConsumingSegmentAssignment().containsKey("consuming_and_offline"));
    assertTrue(assignment2.getConsumingSegmentAssignment().containsKey("consuming_only"));

    assertEquals(assignment2.getOfflineSegmentAssignment().size(), 2);
    assertTrue(assignment2.getOfflineSegmentAssignment().containsKey("offline_and_offline"));
    assertTrue(assignment2.getOfflineSegmentAssignment().containsKey("offline_only"));

    // Test case 3: Empty segment assignment
    Map<String, Map<String, String>> emptyAssignment = new TreeMap<>();
    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment assignment3 =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(emptyAssignment, null);

    assertEquals(assignment3.getCompletedSegmentAssignment().size(), 0);
    assertEquals(assignment3.getConsumingSegmentAssignment().size(), 0);
    assertEquals(assignment3.getOfflineSegmentAssignment().size(), 0);

    // Test case 4: Empty committing segments set
    Set<String> emptyCommittingSegments = new HashSet<>();
    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment assignment4 =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(
            segmentAssignment, emptyCommittingSegments);

    // Should behave the same as null committing segments
    assertEquals(assignment4.getCompletedSegmentAssignment().size(), 5);
    assertEquals(assignment4.getConsumingSegmentAssignment().size(), 3);
    assertEquals(assignment4.getOfflineSegmentAssignment().size(), 2);
  }
}
