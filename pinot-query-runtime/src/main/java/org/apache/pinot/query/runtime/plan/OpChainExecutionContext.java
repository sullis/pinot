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
package org.apache.pinot.query.runtime.plan;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 *  The {@code OpChainExecutionContext} class contains the information derived from the PlanRequestContext.
 *  Members of this class should not be changed once initialized.
 *  This information is then used by the OpChain to create the Operators for a query.
 */
public class OpChainExecutionContext {

  private final MailboxService _mailboxService;
  private final long _requestId;
  private final long _deadlineMs;
  private final Map<String, String> _opChainMetadata;
  private final StageMetadata _stageMetadata;
  private final WorkerMetadata _workerMetadata;
  private final VirtualServerAddress _server;
  private final OpChainId _id;
  @Nullable
  private final PipelineBreakerResult _pipelineBreakerResult;
  private final boolean _traceEnabled;
  @Nullable
  private final ThreadExecutionContext _parentContext;
  @Nullable
  private ServerPlanRequestContext _leafStageContext;
  private final boolean _sendStats;


  public OpChainExecutionContext(MailboxService mailboxService, long requestId, long deadlineMs,
      Map<String, String> opChainMetadata, StageMetadata stageMetadata, WorkerMetadata workerMetadata,
      @Nullable PipelineBreakerResult pipelineBreakerResult, @Nullable ThreadExecutionContext parentContext,
      boolean sendStats) {
    _mailboxService = mailboxService;
    _requestId = requestId;
    _deadlineMs = deadlineMs;
    _opChainMetadata = Collections.unmodifiableMap(opChainMetadata);
    _stageMetadata = stageMetadata;
    _workerMetadata = workerMetadata;
    _sendStats = sendStats;
    _server =
        new VirtualServerAddress(mailboxService.getHostname(), mailboxService.getPort(), workerMetadata.getWorkerId());
    _id = new OpChainId(requestId, workerMetadata.getWorkerId(), stageMetadata.getStageId());
    _pipelineBreakerResult = pipelineBreakerResult;
    _traceEnabled = Boolean.parseBoolean(opChainMetadata.get(CommonConstants.Broker.Request.TRACE));
    _parentContext = parentContext;
  }

  public MailboxService getMailboxService() {
    return _mailboxService;
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getStageId() {
    return _stageMetadata.getStageId();
  }

  public int getWorkerId() {
    return _workerMetadata.getWorkerId();
  }

  public VirtualServerAddress getServer() {
    return _server;
  }

  public long getDeadlineMs() {
    return _deadlineMs;
  }

  public Map<String, String> getOpChainMetadata() {
    return _opChainMetadata;
  }

  public StageMetadata getStageMetadata() {
    return _stageMetadata;
  }

  public WorkerMetadata getWorkerMetadata() {
    return _workerMetadata;
  }

  public OpChainId getId() {
    return _id;
  }

  @Nullable
  public PipelineBreakerResult getPipelineBreakerResult() {
    return _pipelineBreakerResult;
  }

  public boolean isTraceEnabled() {
    return _traceEnabled;
  }

  @Nullable
  public ServerPlanRequestContext getLeafStageContext() {
    return _leafStageContext;
  }

  public void setLeafStageContext(ServerPlanRequestContext leafStageContext) {
    _leafStageContext = leafStageContext;
  }

  @Nullable
  public ThreadExecutionContext getParentContext() {
    return _parentContext;
  }

  public boolean isSendStats() {
    return _sendStats;
  }
}
