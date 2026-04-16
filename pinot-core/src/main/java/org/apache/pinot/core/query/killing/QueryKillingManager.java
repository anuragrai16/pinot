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
package org.apache.pinot.core.query.killing;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.core.query.killing.strategy.ScanEntriesThresholdStrategy;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Central manager for scan-based query killing.
 *
 * <p>The strategy is built once at init via a {@link QueryKillingStrategyFactory}
 * and rebuilt when config changes. The default factory is
 * {@link ScanEntriesThresholdStrategy.Factory}.</p>
 *
 * <p>Guard rails: enabled check, duplicate-kill prevention, log-only mode,
 * framework isolation (try-catch around all evaluation).</p>
 */
public class QueryKillingManager implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryKillingManager.class);

  private static volatile QueryKillingManager _instance;

  private final AtomicReference<QueryMonitorConfig> _configRef;
  private final ServerMetrics _serverMetrics;

  @Nullable
  private volatile QueryKillingStrategy _strategy;

  public QueryKillingManager(AtomicReference<QueryMonitorConfig> configRef, ServerMetrics serverMetrics) {
    _configRef = configRef;
    _serverMetrics = serverMetrics;
  }

  /**
   * Initializes the singleton from a scheduler PinotConfiguration.
   * Called from BaseServerStarter during server startup — independent of accounting factory.
   */
  public static QueryKillingManager init(PinotConfiguration schedulerConfig, ServerMetrics serverMetrics) {
    long maxHeapSize = Runtime.getRuntime().maxMemory();
    QueryMonitorConfig config = new QueryMonitorConfig(schedulerConfig, maxHeapSize);
    AtomicReference<QueryMonitorConfig> configRef = new AtomicReference<>(config);
    QueryKillingManager manager = new QueryKillingManager(configRef, serverMetrics);
    manager.rebuildStrategy();
    _instance = manager;
    return manager;
  }

  @Nullable
  public static QueryKillingManager getInstance() {
    return _instance;
  }

  /**
   * Rebuilds the strategy from the current config. Called at init and when
   * cluster config changes via {@link #onChange}.
   */
  public void rebuildStrategy() {
    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      _strategy = null;
      return;
    }

    try {
      QueryKillingStrategyFactory factory = loadFactory(config);
      _strategy = factory.create(config);
      if (_strategy == null) {
        LOGGER.warn("Scan-based killing is enabled but strategy factory '{}' returned null — "
            + "required configuration may be missing. Scan-based killing will be effectively disabled.",
            factory.getName());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to initialize scan-based killing strategy. "
          + "Scan-based killing will be disabled.", e);
      _strategy = null;
    }
  }

  /**
   * Handles dynamic ZK config changes. Rebuilds QueryMonitorConfig and strategy.
   */
  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    Set<String> filteredChangedConfigs = changedConfigs.stream()
        .filter(c -> c.startsWith(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX))
        .map(c -> c.replace(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".", ""))
        .collect(Collectors.toSet());

    if (filteredChangedConfigs.isEmpty()) {
      return;
    }

    Map<String, String> filteredClusterConfigs = clusterConfigs.entrySet().stream()
        .filter(e -> e.getKey().startsWith(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX))
        .collect(Collectors.toMap(
            e -> e.getKey().replace(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".", ""),
            Map.Entry::getValue));

    QueryMonitorConfig oldConfig = _configRef.get();
    QueryMonitorConfig newConfig = new QueryMonitorConfig(oldConfig, filteredChangedConfigs, filteredClusterConfigs);
    _configRef.set(newConfig);
    rebuildStrategy();
    LOGGER.info("Scan-based killing config updated: mode={}, maxEntriesScannedInFilter={}, maxDocsScanned={}",
        newConfig.getScanBasedKillingMode(),
        newConfig.getScanBasedKillingMaxEntriesScannedInFilter(),
        newConfig.getScanBasedKillingMaxDocsScanned());
  }

  private QueryKillingStrategyFactory loadFactory(QueryMonitorConfig config) {
    String factoryClassName = config.getScanBasedKillingStrategyFactoryClassName();
    if (factoryClassName != null && !factoryClassName.isEmpty()) {
      LOGGER.info("Loading custom query killing strategy factory: {}", factoryClassName);
      try {
        return (QueryKillingStrategyFactory) Class.forName(factoryClassName)
            .getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        LOGGER.error("Failed to load custom strategy factory '{}', falling back to default",
            factoryClassName, e);
      }
    }
    return new ScanEntriesThresholdStrategy.Factory();
  }

  @Nullable
  public QueryKillingStrategy getActiveStrategy() {
    return _strategy;
  }

  /**
   * Resolves the per-query strategy with table-level overrides applied.
   * Called once at query init, result is cached on QueryExecutionContext.
   */
  @Nullable
  public QueryKillingStrategy resolveQueryStrategy(
      @Nullable org.apache.pinot.spi.config.table.QueryConfig queryConfig) {
    QueryKillingStrategy strategy = _strategy;
    if (strategy == null) {
      return null;
    }
    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      return null;
    }
    return strategy.forQuery(queryConfig, config);
  }

  /**
   * Convenience overload called from BaseOperator.checkTermination().
   * Reads cached strategy from the execution context.
   */
  public void checkAndKillIfNeeded(QueryExecutionContext executionContext,
      QueryScanCostContext scanCostContext) {
    QueryKillingStrategy strategy = _strategy;
    if (strategy == null) {
      return;
    }

    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      return;
    }

    if (executionContext.getTerminateException() != null) {
      return;
    }

    try {
      QueryKillingStrategy queryStrategy;
      String configSource;
      Object cached = executionContext.getCachedKillingStrategy();
      if (cached instanceof QueryKillingStrategy) {
        queryStrategy = (QueryKillingStrategy) cached;
        configSource = (queryStrategy != strategy) ? "table:" + executionContext.getTableName() : "cluster";
      } else {
        queryStrategy = strategy;
        configSource = "cluster";
      }

      if (!queryStrategy.shouldTerminate(scanCostContext)) {
        return;
      }

      String queryId = executionContext.getQueryId() != null ? executionContext.getQueryId() : "unknown";
      String tableName = executionContext.getTableName() != null ? executionContext.getTableName() : "unknown";
      long requestId = executionContext.getRequestId();

      QueryKillReport report = queryStrategy.buildKillReport(scanCostContext, requestId, queryId, tableName,
          configSource);

      if (config.isScanBasedKillingLogOnly()) {
        LOGGER.info("SCAN_KILL_DRY_RUN: {}", report.toInternalLogMessage());
        _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1);
        return;
      }

      LOGGER.warn("SCAN_KILL: {}", report.toInternalLogMessage());
      executionContext.terminate(queryStrategy.getErrorCode(), report.toCustomerMessage());
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1);
    } catch (Exception e) {
      LOGGER.error("Error in scan-based killing evaluation for query {}", executionContext.getQueryId(), e);
    }
  }

  /**
   * Full-parameter overload for direct calls (e.g., from tests).
   */
  public void checkAndKillIfNeeded(QueryExecutionContext executionContext,
      QueryScanCostContext scanCostContext, String queryId, String tableName,
      @Nullable org.apache.pinot.spi.config.table.QueryConfig queryConfig) {
    QueryKillingStrategy strategy = _strategy;
    if (strategy == null) {
      return;
    }

    QueryMonitorConfig config = _configRef.get();
    if (config == null || !config.isScanBasedKillingEnabled()) {
      return;
    }

    if (executionContext.getTerminateException() != null) {
      return;
    }

    try {
      QueryKillingStrategy queryStrategy = strategy.forQuery(queryConfig, config);
      String configSource = (queryStrategy != strategy) ? "table:" + tableName : "cluster";

      if (!queryStrategy.shouldTerminate(scanCostContext)) {
        return;
      }

      QueryKillReport report = queryStrategy.buildKillReport(scanCostContext, executionContext.getRequestId(),
          queryId, tableName, configSource);

      if (config.isScanBasedKillingLogOnly()) {
        LOGGER.info("SCAN_KILL_DRY_RUN: {}", report.toInternalLogMessage());
        _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN_DRY_RUN, 1);
        return;
      }

      LOGGER.warn("SCAN_KILL: {}", report.toInternalLogMessage());
      executionContext.terminate(queryStrategy.getErrorCode(), report.toCustomerMessage());
      _serverMetrics.addMeteredGlobalValue(ServerMeter.QUERIES_KILLED_SCAN, 1);
    } catch (Exception e) {
      LOGGER.error("Error in scan-based killing evaluation for query {}", queryId, e);
    }
  }
}
