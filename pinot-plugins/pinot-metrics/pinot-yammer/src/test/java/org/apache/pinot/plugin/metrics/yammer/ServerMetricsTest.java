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
package org.apache.pinot.plugin.metrics.yammer;

import com.yammer.metrics.core.MetricsRegistry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class ServerMetricsTest {
  @Test
  public void testYammerMetrics() {

    PinotConfiguration metricsConfig = new PinotConfiguration();
    metricsConfig.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, YammerMetricsFactory.class.getName());

    PinotMetricUtils.init(metricsConfig);

    YammerMetricsRegistry metricsRegistry = new YammerMetricsRegistry();
    MetricsRegistry yammerRegistry = (MetricsRegistry) metricsRegistry.getMetricsRegistry();

    final String key = "foobar";

    ServerMetrics serverMetrics = new ServerMetrics(metricsRegistry);
    serverMetrics.addMeteredValue(key, ServerMeter.QUERIES, 33);
    serverMetrics.addMeteredValue(key, ServerMeter.QUERIES_KILLED, 1);

    PinotMeter queries = serverMetrics.getMeteredValue(ServerMeter.QUERIES);
    assertEquals(TimeUnit.SECONDS, queries.rateUnit());
    com.yammer.metrics.core.Meter yammerQueries = (com.yammer.metrics.core.Meter) queries.getMetric();
    assertNotNull(yammerQueries);

    final Set<String> allNames = yammerRegistry.allMetrics()
        .keySet()
        .stream()
        .map(mname -> mname.getName())
        .collect(
        Collectors.toSet());

    assertEquals(
        Set.of("pinot.server.foobar.queriesKilled", "pinot.server.foobar.queries", "pinot.server.queries"),
        allNames);
  }
}
