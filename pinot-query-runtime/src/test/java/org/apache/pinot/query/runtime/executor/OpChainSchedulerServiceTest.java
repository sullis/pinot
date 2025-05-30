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
package org.apache.pinot.query.runtime.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class OpChainSchedulerServiceTest {

  private ExecutorService _executor;
  private AutoCloseable _mocks;

  private MultiStageOperator _operatorA;

  @BeforeClass
  public void beforeClass() {
    _mocks = MockitoAnnotations.openMocks(this);
    _executor = Executors.newCachedThreadPool(new NamedThreadFactory("worker_on_" + getClass().getSimpleName()));
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    _mocks.close();
    ExecutorServiceUtils.close(_executor);
  }

  @BeforeMethod
  public void beforeMethod() {
    _operatorA = Mockito.mock(MultiStageOperator.class);
    clearInvocations(_operatorA);
  }

  private OpChain getChain(MultiStageOperator operator) {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, ImmutableMap.of(), ImmutableMap.of());
    OpChainExecutionContext context =
        new OpChainExecutionContext(mailboxService, 123L, Long.MAX_VALUE, ImmutableMap.of(),
            new StageMetadata(0, ImmutableList.of(workerMetadata), ImmutableMap.of()), workerMetadata, null, null,
            true);
    return new OpChain(context, operator);
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredAfterStart()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return SuccessMseBlock.INSTANCE;
    });

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredBeforeStart()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return SuccessMseBlock.INSTANCE;
    });

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCloseOnOperatorsThatFinishSuccessfully()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).close();

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCancelOnOperatorsThatReturnErrorBlock()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(ErrorMseBlock.fromException(new RuntimeException("foo")));
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    schedulerService.register(opChain);

    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCancelOnOpChainsWhenItIsCancelledByDispatch()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch opChainStarted = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      opChainStarted.countDown();
      while (true) {
        Thread.sleep(1000);
      }
    }).when(_operatorA).nextBlock();

    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(1)).when(_operatorA).calculateStats();

    schedulerService.register(opChain);

    Assert.assertTrue(opChainStarted.await(10, TimeUnit.SECONDS), "op chain doesn't seem to be started");

    // now cancel the request.
    schedulerService.cancel(123);

    Assert.assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
  }

  @Test
  public void shouldCallCancelOnOpChainsThatThrow()
      throws InterruptedException {
    OpChain opChain = getChain(_operatorA);
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenThrow(new RuntimeException("foo"));
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    schedulerService.register(opChain);

    Assert.assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
  }
}
