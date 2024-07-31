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
package org.apache.pinot.plugin.metrics.dropwizard;

import com.codahale.metrics.MetricRegistry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  public void testDropWizardMetrics() {

    PinotConfiguration metricsConfig = new PinotConfiguration();
    metricsConfig.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, DropwizardMetricsFactory.class.getName());

    PinotMetricUtils.init(metricsConfig);

    DropwizardMetricsRegistry metricsRegistry = new DropwizardMetricsRegistry();
    MetricRegistry dwRegistry = (MetricRegistry) metricsRegistry.getMetricsRegistry();

    final String key = "foobar";

    ServerMetrics serverMetrics = new ServerMetrics(metricsRegistry);
    serverMetrics.addMeteredValue(key, ServerMeter.QUERIES, 33);
    serverMetrics.addMeteredValue(key, ServerMeter.QUERIES_KILLED, 1);

    PinotMeter queries = serverMetrics.getMeteredValue(ServerMeter.QUERIES);
    assertEquals(TimeUnit.NANOSECONDS, queries.rateUnit());
    com.codahale.metrics.Meter dwQueries = (com.codahale.metrics.Meter) queries.getMetric();
    assertNotNull(dwQueries);

    assertEquals(
        Set.of("pinot.server.foobar.queriesKilled", "pinot.server.foobar.queries", "pinot.server.queries"),
        dwRegistry.getMetrics().keySet());
  }
}
