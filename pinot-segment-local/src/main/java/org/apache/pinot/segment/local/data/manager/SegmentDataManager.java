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
package org.apache.pinot.segment.local.data.manager;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Base segment data manager to maintain reference count for the segment.
 */
public abstract class SegmentDataManager {
  private final long _loadTimeMs = System.currentTimeMillis();
  private final AtomicBoolean _offloaded = new AtomicBoolean();
  private final AtomicBoolean _destroyed = new AtomicBoolean();
  private int _referenceCount = 1;

  public long getLoadTimeMs() {
    return _loadTimeMs;
  }

  public synchronized int getReferenceCount() {
    return _referenceCount;
  }

  /**
   * Increases the reference count. Should be called when acquiring the segment.
   *
   * @return Whether the segment is still valid (i.e. reference count not 0)
   */
  public synchronized boolean increaseReferenceCount() {
    if (_referenceCount == 0) {
      return false;
    } else {
      _referenceCount++;
      return true;
    }
  }

  /**
   * Decreases the reference count. Should be called when releasing or dropping the segment.
   *
   * @return Whether the segment can be destroyed (i.e. reference count is 0)
   */
  public synchronized boolean decreaseReferenceCount() {
    if (_referenceCount <= 1) {
      _referenceCount = 0;
      return true;
    } else {
      _referenceCount--;
      return false;
    }
  }

  public abstract String getSegmentName();

  public abstract IndexSegment getSegment();

  public boolean hasMultiSegments() {
    return false;
  }

  public List<IndexSegment> getSegments() {
    return Collections.emptyList();
  }

  /**
   * Offloads the segment from the metadata management (e.g. upsert metadata), but not releases the resources yet
   * because there might be queries still accessing the segment.
   */
  public void offload() {
    if (_offloaded.compareAndSet(false, true)) {
      doOffload();
    }
  }

  public abstract void doOffload();

  /**
   * Destroys the data manager and releases all the resources allocated.
   * The data manager can only be destroyed once.
   */
  public void destroy() {
    // NOTE: We want the test to catch the case when destroy is called without offloading, but not fail the production.
    assert _offloaded.get() : "Cannot destroy segment data manager without offloading it first";
    offload();

    if (_destroyed.compareAndSet(false, true)) {
      doDestroy();
    }
  }

  protected abstract void doDestroy();
}
