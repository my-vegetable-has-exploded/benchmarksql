package com.github.pgsqlio.benchmarksql.jtpcc;

import java.util.HashMap;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PerformConfig {
    private static Logger logger = LogManager.getLogger(SystemConfig.class);

    public int numWarehouses;
    public int useWarehouseFrom = -1;
    public int useWarehouseTo = -1;
    public int numMonkeys = 8;
    public int numSUTThreads = 32;
    public int maxDeliveryBGThreads = 0;
    public int maxDeliveryBGPerWH = 0;
    public int rampupMins = 1;
    public int runMins;
    public int rampupSUTMins = 1;
    public int rampupTerminalMins = 0;
    public int reportIntervalSecs = 1;
    public int resultIntervalSecs = 1;
    public double restartSUTThreadProb = 0;
    public double keyingTimeMultiplier = 0.1;
    public double thinkTimeMultiplier = 0.1;
    public int terminalMultiplier = 1;
    public boolean traceTerminalIO = false;
    public double paymentWeight = 0;
    public double orderStatusWeight = 0;
    public double deliveryWeight = 0;
    public double stockLevelWeight = 0;
    public double storeWeight = 0;
    public double rollbackPercent = 0;
    

    String getValFromYamlMap(HashMap<String, Object> yamlMap, String key, String defVal) {
        Object val = yamlMap.get(key);
        String result = val != null ? val.toString() : defVal;
        logger.info("main, {}={}", key, result);
        return result;
    }

    String getValFromYamlMap(HashMap<String, Object> yamlMap, String key) {
        Object val = yamlMap.get(key);
        String result = val != null ? val.toString() : null;
        logger.info("main, {}={}", key, result);
        return result;
    }

    public void updatePerformConfigWithYamlMap(HashMap<String, Object> yamlMap) {
        if (yamlMap.containsKey("warehouses")) {
            numWarehouses = Integer.parseInt(getValFromYamlMap(yamlMap, "warehouses"));
        }
        if (yamlMap.containsKey("useWarehouseFrom")) {
            useWarehouseFrom = Integer.parseInt(getValFromYamlMap(yamlMap, "useWarehouseFrom"));
        }
        if (yamlMap.containsKey("useWarehouseTo")) {
            useWarehouseTo = Integer.parseInt(getValFromYamlMap(yamlMap, "useWarehouseTo"));
        }
        if (yamlMap.containsKey("monkeys")) {
            numMonkeys = Integer.parseInt(getValFromYamlMap(yamlMap, "monkeys"));
        }
        if (yamlMap.containsKey("sutThreads")) {
            numSUTThreads = Integer.parseInt(getValFromYamlMap(yamlMap, "sutThreads"));
        }
        if (yamlMap.containsKey("maxDeliveryBGThreads")) {
            maxDeliveryBGThreads = Integer.parseInt(getValFromYamlMap(yamlMap, "maxDeliveryBGThreads"));
        }
        if (yamlMap.containsKey("maxDeliveryBGPerWarehouse")) {
            maxDeliveryBGPerWH = Integer.parseInt(getValFromYamlMap(yamlMap, "maxDeliveryBGPerWarehouse"));
        }
        if (yamlMap.containsKey("rampupMins")) {
            rampupMins = Integer.parseInt(getValFromYamlMap(yamlMap, "rampupMins"));
        }
        if (yamlMap.containsKey("runMins")) {
            runMins = Integer.parseInt(getValFromYamlMap(yamlMap, "runMins"));
        }
        if (yamlMap.containsKey("rampupSUTMins")) {
            rampupSUTMins = Integer.parseInt(getValFromYamlMap(yamlMap, "rampupSUTMins"));
        }
        if (yamlMap.containsKey("rampupTerminalMins")) {
            rampupTerminalMins = Integer.parseInt(getValFromYamlMap(yamlMap, "rampupTerminalMins"));
        }
        if (yamlMap.containsKey("reportIntervalSecs")) {
            reportIntervalSecs = Integer.parseInt(getValFromYamlMap(yamlMap, "reportIntervalSecs"));
        }
        if (yamlMap.containsKey("resultIntervalSecs")) {
            resultIntervalSecs = Integer.parseInt(getValFromYamlMap(yamlMap, "resultIntervalSecs"));
        }
        if (yamlMap.containsKey("restartSUTThreadProbability")) {
            restartSUTThreadProb = Double.parseDouble(getValFromYamlMap(yamlMap, "restartSUTThreadProbability"));
        }
        if (yamlMap.containsKey("keyingTimeMultiplier")) {
            keyingTimeMultiplier = Double.parseDouble(getValFromYamlMap(yamlMap, "keyingTimeMultiplier"));
        }
        if (yamlMap.containsKey("thinkTimeMultiplier")) {
            thinkTimeMultiplier = Double.parseDouble(getValFromYamlMap(yamlMap, "thinkTimeMultiplier"));
        }
        if (yamlMap.containsKey("terminalMultiplier")) {
            terminalMultiplier = Integer.parseInt(getValFromYamlMap(yamlMap, "terminalMultiplier"));
        }
        if (yamlMap.containsKey("traceTerminalIO")) {
            traceTerminalIO = Boolean.parseBoolean(getValFromYamlMap(yamlMap, "traceTerminalIO"));
        }
        if (yamlMap.containsKey("paymentWeight")) {
            paymentWeight = Double.parseDouble(getValFromYamlMap(yamlMap, "paymentWeight"));
        }
        if (yamlMap.containsKey("orderStatusWeight")) {
            orderStatusWeight = Double.parseDouble(getValFromYamlMap(yamlMap, "orderStatusWeight"));
        }
        if (yamlMap.containsKey("deliveryWeight")) {
            deliveryWeight = Double.parseDouble(getValFromYamlMap(yamlMap, "deliveryWeight"));
        }
        if (yamlMap.containsKey("stockLevelWeight")) {
            stockLevelWeight = Double.parseDouble(getValFromYamlMap(yamlMap, "stockLevelWeight"));
        }
        if (yamlMap.containsKey("storeWeight")) {
            storeWeight = Double.parseDouble(getValFromYamlMap(yamlMap, "storeWeight"));
        }
        if (yamlMap.containsKey("rollbackPercent")) {
            rollbackPercent = Double.parseDouble(getValFromYamlMap(yamlMap, "rollbackPercent"));
        }
    }

    public PerformConfig(HashMap<String, Object> yamlMap) {
        updatePerformConfigWithYamlMap(yamlMap);
    }

    private String getProp(Properties p, String pName) {
        String prop = p.getProperty(pName);
        logger.info("main, {}={}", pName, prop);
        return (prop);
    }

    public void updatePerformConfigWithProperties(Properties p) {
        if (p.containsKey("warehouses")) {
            numWarehouses = Integer.parseInt(getProp(p, "warehouses"));
        }
        if (p.containsKey("useWarehouseFrom")) {
            useWarehouseFrom = Integer.parseInt(getProp(p, "useWarehouseFrom"));
        }
        if (p.containsKey("useWarehouseTo")) {
            useWarehouseTo = Integer.parseInt(getProp(p, "useWarehouseTo"));
        }
        if (p.containsKey("monkeys")) {
            numMonkeys = Integer.parseInt(getProp(p, "monkeys"));
        }
        if (p.containsKey("sutThreads")) {
            numSUTThreads = Integer.parseInt(getProp(p, "sutThreads"));
        }
        if (p.containsKey("maxDeliveryBGThreads")) {
            maxDeliveryBGThreads = Integer.parseInt(getProp(p, "maxDeliveryBGThreads"));
        }
        if (p.containsKey("maxDeliveryBGPerWarehouse")) {
            maxDeliveryBGPerWH = Integer.parseInt(getProp(p, "maxDeliveryBGPerWarehouse"));
        }
        if (p.containsKey("rampupMins")) {
            rampupMins = Integer.parseInt(getProp(p, "rampupMins"));
        }
        if (p.containsKey("runMins")) {
            runMins = Integer.parseInt(getProp(p, "runMins"));
        }
        if (p.containsKey("rampupSUTMins")) {
            rampupSUTMins = Integer.parseInt(getProp(p, "rampupSUTMins"));
        }
        if (p.containsKey("rampupTerminalMins")) {
            rampupTerminalMins = Integer.parseInt(getProp(p, "rampupTerminalMins"));
        }
        if (p.containsKey("reportIntervalSecs")) {
            reportIntervalSecs = Integer.parseInt(getProp(p, "reportIntervalSecs"));
        }
        if (p.containsKey("resultIntervalSecs")) {
            resultIntervalSecs = Integer.parseInt(getProp(p, "resultIntervalSecs"));
        }
        if (p.containsKey("restartSUTThreadProbability")) {
            restartSUTThreadProb = Double.parseDouble(getProp(p, "restartSUTThreadProbability"));
        }
        if (p.containsKey("keyingTimeMultiplier")) {
            keyingTimeMultiplier = Double.parseDouble(getProp(p, "keyingTimeMultiplier"));
        }
        if (p.containsKey("thinkTimeMultiplier")) {
            thinkTimeMultiplier = Double.parseDouble(getProp(p, "thinkTimeMultiplier"));
        }
        if (p.containsKey("terminalMultiplier")) {
            terminalMultiplier = Integer.parseInt(getProp(p, "terminalMultiplier"));
        }
        if (p.containsKey("traceTerminalIO")) {
            traceTerminalIO = Boolean.parseBoolean(getProp(p, "traceTerminalIO"));
        }
        if (p.containsKey("paymentWeight")) {
            paymentWeight = Double.parseDouble(getProp(p, "paymentWeight"));
        }
        if (p.containsKey("orderStatusWeight")) {
            orderStatusWeight = Double.parseDouble(getProp(p, "orderStatusWeight"));
        }
        if (p.containsKey("deliveryWeight")) {
            deliveryWeight = Double.parseDouble(getProp(p, "deliveryWeight"));
        }
        if (p.containsKey("stockLevelWeight")) {
            stockLevelWeight = Double.parseDouble(getProp(p, "stockLevelWeight"));
        }
        if (p.containsKey("storeWeight")) {
            storeWeight = Double.parseDouble(getProp(p, "storeWeight"));
        }
        if (p.containsKey("rollbackPercent")) {
            rollbackPercent = Double.parseDouble(getProp(p, "rollbackPercent"));
        }
    }

    public PerformConfig(Properties p) {
        updatePerformConfigWithProperties(p);
    }
}