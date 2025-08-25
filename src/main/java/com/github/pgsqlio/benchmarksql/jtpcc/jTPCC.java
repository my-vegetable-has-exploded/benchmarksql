package com.github.pgsqlio.benchmarksql.jtpcc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import com.github.pgsqlio.benchmarksql.application.AppCRDB;
import com.github.pgsqlio.benchmarksql.application.AppGeneric;
import com.github.pgsqlio.benchmarksql.application.oracle.AppOracleStoredProc;
import com.github.pgsqlio.benchmarksql.application.postgres.AppPostgreSQLStoredProc;
import com.github.pgsqlio.benchmarksql.chaos.ChaosInjecter;
import com.github.pgsqlio.benchmarksql.oscollector.OSCollector;

/**
 * jTPCC - BenchmarkSQL main class
 */
public class jTPCC {
  private static Logger log = LogManager.getLogger(jTPCC.class);

  private long now;

  public jTPCCRandom rnd;
  public String applicationName;
  public String iDBType;
  public String iConn;
  public String iUser;
  public String iPassword;

  public boolean isSkewed = false;
  public SkewRandom skewRand = null;

  public double distributedRatio = 0.01;
  public int distributedNodes = 1;

  public static int loadWarehouses;
  public static int loadNuRandCLast;
  public static int loadNuRandCC_ID;
  public static int loadNuRandCI_ID;

  public static int dbType;
  public static int numWarehouses;
  public static int useWarehouses;
  public static int useWarehouseFrom;
  public static int useWarehouseTo;
  public static int numMonkeys;
  public static int numSUTThreads;
  public static int maxDeliveryBGThreads;
  public static int maxDeliveryBGPerWH;
  public static int runMins;
  public static int rampupMins;
  public static int rampupSUTMins;
  public static int rampupTerminalMins;
  public static int reportIntervalSecs;
  public static int resultIntervalSecs;
  public static double restartSUTThreadProb;
  public static double keyingTimeMultiplier;
  public static double thinkTimeMultiplier;
  public static int terminalMultiplier;
  public static boolean traceTerminalIO = false;

  public static int sutThreadDelay;
  public static int terminalDelay;
  public static int numTerms;

  public static double newOrderWeight;
  public static double paymentWeight;
  public static double orderStatusWeight;
  public static double deliveryWeight;
  public static double stockLevelWeight;
  public static double storeWeight;
  public static double rollbackPercent;

  private OSCollector osCollector = null;
  private jTPCCTData terminal_data[];
  private Thread scheduler_thread;
  static public jTPCCScheduler scheduler;
  public jTPCCSUT systemUnderTest;
  public jTPCCMonkey monkeys;

  public SystemConfig sysConfig;
  public Connection sqliteConn;
  PerformConfig performConfig;

  public static String resultDirectory = null;
  public static String osCollectorScript = null;
  public static String reportScript = null;
  private static String resultDirName = null;
  private static BufferedWriter summaryCSV = null;
  private static BufferedWriter histogramCSV = null;
  private static BufferedWriter resultCSV = null;
  private static BufferedWriter runInfoCSV = null;
  private static BufferedWriter traceCSV = null;
  private static BufferedWriter faultInfoCSV = null;
  private static BufferedWriter txnlogCSV = null;
  public static int runID = 0;
  public static long csv_begin;
  public static long result_begin;
  public static long result_end;
  private static Object result_lock;
  private static Object trace_lock;
  private static Object fault_lock;

  public static void main(String args[]) throws FileNotFoundException {
    new jTPCC();
  }

  private String getProp(Properties p, String pName) {
    String prop = p.getProperty(pName);
    log.info("main, {}={}", pName, prop);
    return (prop);
  }

  boolean useYaml = false;
  private HashMap<String, Object> yamlMap;
  private Properties ini;

  String getSysVal(String key) {
    if (useYaml) {
      Object val = yamlMap.get(key);
      String result = val != null ? val.toString() : null;
      log.info("main, {}={}", key, result);
      return result;
    } else {
      return getProp(ini, key);
    }
  }

  /*
   * Transform all key-valuas paiers in the yamlMap (FaultTemplate) to the faultInfoMap,
   * if the key starts with "fault." (config of new version), remove the prefix "fault."
   */
  HashMap<String, Object> getFaultInfoMap(HashMap<String, Object> nYamlMap) {
    HashMap<String, Object> faultInfoMap = new HashMap<String, Object>();
    for (String key : nYamlMap.keySet()) {
      Object valObj = nYamlMap.get(key);
      String valStr = valObj != null ? valObj.toString() : null;
      if (key.startsWith("fault.")) {
        log.info("main, faultInfoMap, {}={}", key, valStr);
        faultInfoMap.put(key.substring(6), valStr);
      } else {
        faultInfoMap.put(key, valStr);
      }
    }
    return faultInfoMap;
  }

  HashMap<String, Object> detailedFaultInfo(HashMap<String, Object> yamlMap, String faultFileName) {
    HashMap<String, Object> detailFaultInfo = new HashMap<String, Object>();
    String fault_type = yamlMap.get("template").toString();
    // remove .yaml tail
    fault_type = fault_type.substring(0, fault_type.length() - 5);
    log.info("main, fault type={}", fault_type);
    // get $zone.leader-compute-0 and remove $
    String fault_scope = yamlMap.get("injectpods").toString().substring(1);
    // split fault_scope by - and get scope 、role and num
    String[] scopeParts = fault_scope.split("-");
    String scope = scopeParts[0].substring(5); // remove "zone."
    String role = scopeParts[1];
    int num = Integer.parseInt(scopeParts[2]);
    log.info("main, fault scope={}, role={}, num={}", scope, role, num);
    detailFaultInfo.put("fault_type", fault_type);
    detailFaultInfo.put("scope", scope);
    detailFaultInfo.put("role", role);
    detailFaultInfo.put("num", num);
    detailFaultInfo.put("fault_filename", faultFileName);
    
    // Extract fault-specific parameters
    StringBuilder faultParams = new StringBuilder();
    if ("net_delay".equals(fault_type)) {
      String latency = yamlMap.get("latency") != null ? yamlMap.get("latency").toString() : "None";
      faultParams.append("latency:").append(latency);
    } else if ("io_fault".equals(fault_type)) {
      String percent = yamlMap.get("percent") != null ? yamlMap.get("percent").toString() : "None";
      faultParams.append("percent:").append(percent);
    } else if ("net_loss".equals(fault_type)) {
      String loss = yamlMap.get("loss") != null ? yamlMap.get("loss").toString() : "None";
      faultParams.append("loss:").append(loss);
    } else if ("cpu_stress".equals(fault_type)) {
      String load = yamlMap.get("load") != null ? yamlMap.get("load").toString() : "None";
      faultParams.append("load:").append(load);
    } else if ("fail".equals(fault_type)) {
      faultParams.append("None");
    } else {
      // For other fault types, collect all parameters except template, injectpods, duration
      for (String key : yamlMap.keySet()) {
        if (!key.equals("template") && !key.equals("injectpods") && !key.equals("duration") && !key.equals("volumePath")) {
          if (faultParams.length() > 0) faultParams.append(",");
          faultParams.append(key).append(":").append(yamlMap.get(key).toString());
        }
      }
      if (faultParams.length() == 0) {
        faultParams.append("None");
      }
    }
    detailFaultInfo.put("fault_params", faultParams.toString());
    
    return detailFaultInfo;
  }

  public jTPCC() throws FileNotFoundException {
    StringBuilder sb = new StringBuilder();
    Formatter fmt = new Formatter(sb);

    String propStr = System.getProperty("prop");
    // check the propStr is a property file or a yaml file
    if (propStr.endsWith(".yaml")) {
      useYaml = true;
      log.info("main, loading properties from yaml file: {}", propStr);
      InputStream propInputStream = new FileInputStream(propStr);
      Yaml yaml = new Yaml();
      yamlMap = yaml.load(propInputStream);
      sysConfig = new SystemConfig(yamlMap);
      performConfig = new PerformConfig(yamlMap);
    } else if (propStr.endsWith(".properties")) {
      // load the ini file
      ini = new Properties();
      try {
        ini.load(new FileInputStream(propStr));
      } catch (IOException e) {
        log.error("main, could not load properties file");
      }
      sysConfig = new SystemConfig(ini);
      performConfig = new PerformConfig(ini);
    } else {
      throw new IllegalArgumentException("Invalid property arguments: " + propStr);
    }

    try {
      Class.forName("org.sqlite.JDBC");
      this.sqliteConn = DriverManager.getConnection("jdbc:sqlite:service_data/benchmark.db");
      //  select max runID from batch_runs table
      PreparedStatement stmt = this.sqliteConn.prepareStatement("SELECT MAX(run_id) AS max_runID FROM batch_runs");
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        runID = rs.getInt("max_runID") ;
        log.info("main, runID from sqlite database: {}", runID);
      }
    } catch (Exception e) {
      log.error("main, could not connect to sqlite database: {}", e.getMessage()); 
      System.exit(1);
    }

    HashMap<String, Object> faultInfo = null;
	// check if the fault is available
	try {
      ChaosInjecter injecter = ChaosInjecter.getInstance(this);
      for (String fault : sysConfig.faults) {
        String faultTemplatePath = injecter.getFaultTemplatesWithName(fault);
        Yaml yaml = new Yaml();
        InputStream faultTemplate = new FileInputStream(faultTemplatePath);
        HashMap<String, Object> allParamsMap = yaml.load(faultTemplate);
        performConfig.updatePerformConfigWithYamlMap(allParamsMap);
        HashMap<String, Object> faultInfoMap = getFaultInfoMap(allParamsMap);
        faultInfo = detailedFaultInfo(faultInfoMap, fault);
        injecter.initialFaultWithCombinedConfig(sysConfig, fault, faultInfoMap);
      }
    } catch (Exception e) {
      log.error("main, could not init fault file, " + e.getMessage());
      // Don't exit this case, exit with error code
      System.exit(1);
    }
    
    /*
     * Get all the configuration settings
     */
    log.info("main, ");
    log.info("main, +-------------------------------------------------------------+");
    log.warn("main,      BenchmarkSQL v{}", jTPCCConfig.JTPCCVERSION);
    log.info("main, +-------------------------------------------------------------+");
    log.info("main,  (c) 2003, Raul Barbosa");
    log.info("main,  (c) 2004-2023, Denis Lussier");
    log.info("main,  (c) 2016-2023, Jan Wieck");
    log.info("main, +-------------------------------------------------------------+");
    log.info("main, ");
    String iDBType = getSysVal("db");
    String iDriver = getSysVal("driver");
    applicationName = getSysVal("application");
    iConn = getSysVal("conn");
    iUser = getSysVal("user");
    iPassword = getSysVal("password");

    log.info("main, ");
    numWarehouses = performConfig.numWarehouses;
    useWarehouseFrom = performConfig.useWarehouseFrom;
    useWarehouseTo = performConfig.useWarehouseTo;
    useWarehouses = numWarehouses;
    if (useWarehouseFrom > 0 && useWarehouseTo > 0) {
      useWarehouses = useWarehouseTo - useWarehouseFrom + 1;
    } else {
      useWarehouseFrom = 1;
      useWarehouseTo = useWarehouses;
    }

	isSkewed = performConfig.isSkewed;
	log.info("main, skew={}", isSkewed);
	if (isSkewed) {
		long seed = performConfig.seed;
		double alphaData = performConfig.alphaData;
		double alphaTxn = performConfig.alphaTxn;
		long updateInterval = performConfig.updateInterval;
		skewRand = new SkewRandom(numWarehouses, 10, seed, alphaData, alphaTxn, updateInterval);
	}
    distributedRatio = performConfig.distributedRatio;
    distributedNodes = performConfig.distributedNodes;
    numMonkeys = performConfig.numMonkeys;
    numSUTThreads = performConfig.numSUTThreads;
    maxDeliveryBGThreads = performConfig.maxDeliveryBGThreads;
    maxDeliveryBGPerWH = performConfig.maxDeliveryBGPerWH;
    rampupMins = performConfig.rampupMins;
    runMins = performConfig.runMins;
    rampupSUTMins = performConfig.rampupSUTMins;
    rampupTerminalMins = performConfig.rampupTerminalMins;
    reportIntervalSecs = performConfig.reportIntervalSecs;
    resultIntervalSecs = performConfig.resultIntervalSecs;
    restartSUTThreadProb = performConfig.restartSUTThreadProb;
    keyingTimeMultiplier = performConfig.keyingTimeMultiplier;
    thinkTimeMultiplier = performConfig.thinkTimeMultiplier;
    terminalMultiplier = performConfig.terminalMultiplier;
    traceTerminalIO = performConfig.traceTerminalIO;
    log.info("main, ");
    paymentWeight = performConfig.paymentWeight;
    orderStatusWeight = performConfig.orderStatusWeight;
    deliveryWeight = performConfig.deliveryWeight;
    stockLevelWeight = performConfig.stockLevelWeight;
    storeWeight = performConfig.storeWeight;
    newOrderWeight = 100.0 - paymentWeight - orderStatusWeight - deliveryWeight - stockLevelWeight - storeWeight;
    if (newOrderWeight < 0.0) {
      log.error("main, newOrderWeight is below zero");
      return;
    }
    fmt.format("newOrderWeight=%.3f", newOrderWeight);
    log.info("main, {}", sb.toString());
    log.info("main, ");

    rollbackPercent = performConfig.rollbackPercent;
    log.info("main, ");

    // insert into sqlite table metrics with all the extended information
    try {
      PreparedStatement stmt = this.sqliteConn.prepareStatement(
          "INSERT INTO metrics (run_id, dbtype, fault_filename, fault_type, scope, role, num, fault_params, " +
          "warehouses, new_order_weight, payment_weight, order_status_weight, delivery_weight, " +
          "stock_level_weight, store_weight, alpha_data, alpha_txn, distributed_ratio, distributed_nodes) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      stmt.setInt(1, runID);
      stmt.setString(2, getSysVal("db"));
      stmt.setString(3, faultInfo != null ? faultInfo.get("fault_filename").toString() : "None");
      stmt.setString(4, faultInfo != null ? faultInfo.get("fault_type").toString() : "None");
      stmt.setString(5, faultInfo != null ? faultInfo.get("scope").toString() : "None");
      stmt.setString(6, faultInfo != null ? faultInfo.get("role").toString() : "None");
      stmt.setInt(7, faultInfo != null ? (Integer) faultInfo.get("num") : 0);
      stmt.setString(8, faultInfo != null ? faultInfo.get("fault_params").toString() : "None");
      stmt.setInt(9, numWarehouses);
      
      // Set transaction weights
      stmt.setDouble(10, newOrderWeight);
      stmt.setDouble(11, paymentWeight);
      stmt.setDouble(12, orderStatusWeight); 
      stmt.setDouble(13, deliveryWeight);
      stmt.setDouble(14, stockLevelWeight);
      stmt.setDouble(15, storeWeight);
      
      // Set skew and distributed parameters
      stmt.setDouble(16, isSkewed ? performConfig.alphaData : -1.0);
      stmt.setDouble(17, isSkewed ? performConfig.alphaTxn : -1.0); 
      stmt.setDouble(18, distributedRatio);
      stmt.setInt(19, distributedNodes);
      
      stmt.executeUpdate();
      this.sqliteConn.commit();
    } catch (Exception e) {
      log.error("main, could not insert metrics into sqlite database: {}", e.getMessage());
    }

    numTerms = 10 * terminalMultiplier;
    sutThreadDelay = (rampupSUTMins * 60000) / numSUTThreads;
    terminalDelay = (rampupTerminalMins * 60000) / (useWarehouses * numTerms);


    if (iDBType.equals("oracle"))
      dbType = jTPCCConfig.DB_ORACLE;
    else if (iDBType.equals("postgres"))
      dbType = jTPCCConfig.DB_POSTGRES;
    else if (iDBType.equals("firebird"))
      dbType = jTPCCConfig.DB_FIREBIRD;
    else if (iDBType.equals("mariadb"))
      dbType = jTPCCConfig.DB_MARIADB;
    else if (iDBType.equals("transact-sql"))
      dbType = jTPCCConfig.DB_TSQL;
    else if (iDBType.equals("babelfish"))
      dbType = jTPCCConfig.DB_BABELFISH;
    else if (iDBType.equals("mysql"))
      dbType = jTPCCConfig.DB_MYSQL;
	else if (iDBType.equals("oceanbase"))
	  dbType = jTPCCConfig.DB_OCEANBASE;
	else if (iDBType.equals("tidb"))
	  dbType = jTPCCConfig.DB_TiDB;
    else if (iDBType.equals("polardb"))
      dbType = jTPCCConfig.DB_POLARDB; 
    else if (iDBType.equals("dameng"))
      dbType = jTPCCConfig.DB_DAMENG;
    else if (iDBType.equals("crdb"))
      dbType = jTPCCConfig.DB_CRDB;
    else {
      log.error("Unknown database type '{}'", iDBType);
      return;
    }


    /*
     * Load the requested JDBC driver
     */
    try {
      String driver = iDriver;

      log.info("main, Loading database driver: \'{}\'...", driver);
      Class.forName(iDriver);
    } catch (Exception ex) {
      log.error("main, Unable to load the database driver!");
      log.error("main, {}", ex.getMessage());
      return;
    }

    /*
     * Get the load configuration from the database and clear bmsql_txnlog table
     */
    try {
      Connection dbConn;
      Properties dbProps;
      ResultSet rs;
      PreparedStatement cfgStmt;

      dbProps = new Properties();
      dbProps.setProperty("user", iUser);
      dbProps.setProperty("password", iPassword);
      dbConn = DriverManager.getConnection(iConn, dbProps);
      dbConn.setAutoCommit(false);

      cfgStmt = dbConn.prepareStatement("SELECT cfg_value FROM bmsql_config WHERE cfg_name = ?");

      cfgStmt.setString(1, "warehouses");
      rs = cfgStmt.executeQuery();
      rs.next();
      loadWarehouses = Integer.parseInt(rs.getString("cfg_value"));

      cfgStmt.setString(1, "nURandCLast");
      rs = cfgStmt.executeQuery();
      rs.next();
      loadNuRandCLast = Integer.parseInt(rs.getString("cfg_value"));

      cfgStmt.setString(1, "nURandCC_ID");
      rs = cfgStmt.executeQuery();
      rs.next();
      loadNuRandCC_ID = Integer.parseInt(rs.getString("cfg_value"));

      cfgStmt.setString(1, "nURandCI_ID");
      rs = cfgStmt.executeQuery();
      rs.next();
      loadNuRandCI_ID = Integer.parseInt(rs.getString("cfg_value"));

      cfgStmt.close();
      dbConn.rollback();

	  PreparedStatement txnlogStmt = dbConn.prepareStatement("TRUNCATE TABLE bmsql_txnlog");
	  txnlogStmt.executeUpdate();
	  txnlogStmt.close();
	  dbConn.commit();
      dbConn.close();
    } catch (Exception ex) {
      log.error("main, Unable to read load configuration and clear bmsql_txnlog");
      log.error("main, {}", ex.getMessage());
      return;
    }

    /*
     * Check that we support the requested application implementation
     */
    if (!applicationName.equals("Generic") && !applicationName.equals("PostgreSQLStoredProc")
        && !applicationName.equals("OracleStoredProc") && !applicationName.equals("CRDB")) {
      log.error("Unknown application name '{}'", applicationName);
      return;
    }

    /*
     * We need to have set the start time of the rampup (csv_begin)
     * from here on.
     */
    now = System.currentTimeMillis();
    csv_begin = now;

    /*
     * Launch the OS metric collector if configured
     */
    String resultDirectory = getSysVal("resultDirectory");
    String osCollectorScript = getSysVal("osCollectorScript");

    if (resultDirectory != null) {
      StringBuffer sbRes = new StringBuffer();
      Formatter fmtRes = new Formatter(sbRes);
      Pattern p = Pattern.compile("%t");
      Calendar cal = Calendar.getInstance();

      // String iRunID;

      // iRunID = System.getProperty("runID");
      // if (iRunID != null) {
      //   runID = Integer.parseInt(iRunID);
      // }

      /*
       * Split the resultDirectory into strings around patterns of %t and then insert date/time
       * formatting based on the current time. That way the resultDirectory in the properties file
       * can have date/time format elements like in result_%tY-%tm-%td to embed the current date in
       * the directory name.
       */
      String[] parts = p.split(resultDirectory, -1);
      sbRes.append(parts[0]);
      for (int i = 1; i < parts.length; i++) {
        fmtRes.format("%t" + parts[i].substring(0, 1), cal);
        sbRes.append(parts[i].substring(1));
      }
      resultDirName = sbRes.toString();
      File resultDir = new File(resultDirName);
      File resultDataDir = new File(resultDir, "data");

      // Create the output directory structure.
      if (!resultDir.mkdir()) {
        log.error("Failed to create directory '{}'", resultDir.getPath());
        System.exit(1);
      }
      if (!resultDataDir.mkdir()) {
        log.error("Failed to create directory '{}'", resultDataDir.getPath());
        System.exit(1);
      }

      // Copy the used properties file into the resultDirectory.
      File resultConfigFile = useYaml? new File(resultDir, "run.yaml") : new File(resultDir, "run.properties");
      try {
        copyFile(new File(System.getProperty("prop")), resultConfigFile);
      } catch (Exception e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("main, copied {} to {}", System.getProperty("prop"),
          resultConfigFile.getPath());

      // Create the runInfo.csv file.
      String runInfoCSVName = new File(resultDataDir, "runInfo.csv").getPath();
      try {
        runInfoCSV = new BufferedWriter(new FileWriter(runInfoCSVName));
        runInfoCSV.write("runID,dbType,jTPCCVersion,application," + "rampupMins,runMins,startTS,"
            + "loadWarehouses,runWarehouses,numSUTThreads,"
            + "maxDeliveryBGThreads,maxDeliveryBGPerWarehouse," + "restartSUTThreadProbability,"
            + "thinkTimeMultiplier,keyingTimeMultiplier\n");
        runInfoCSV.write(runID + "," + iDBType + "," + jTPCCConfig.JTPCCVERSION + ","
            + applicationName + "," + rampupMins + "," + runMins + "," + now + "," + loadWarehouses + ","
            + numWarehouses + "," + numSUTThreads + "," + maxDeliveryBGThreads + ","
            + maxDeliveryBGPerWH + "," + restartSUTThreadProb + "," + thinkTimeMultiplier + ","
            + keyingTimeMultiplier + "\n");
        runInfoCSV.close();
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("main, created {} for runID {}", runInfoCSVName, runID);

      // Open the aggregated transaction result.csv file.
      String resultCSVName = new File(resultDataDir, "result.csv").getPath();
      try {
        resultCSV = new BufferedWriter(new FileWriter(resultCSVName));
        resultCSV.write("ttype,second,numtrans," + "sumlatencyms,minlatencyms,maxlatencyms,"
            + "sumdelayms,mindelayms,maxdelayms\n");
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("main, writing aggregated transaction results to {}", resultCSVName);
      result_lock = new Object();

	  // Open the trace.csv file recording transaction details
	  String traceCSVName = new File(resultDataDir, "trace.csv").getPath();
	  try {
		traceCSV = new BufferedWriter(new FileWriter(traceCSVName));
		traceCSV.write("txn_id,ttype,start,end,rollback,error\n");
	  } catch (IOException e) {
		log.error(e.getMessage());
		System.exit(1);
	  }
	  log.info("main, writing transaction trace to {}", traceCSVName);
	  trace_lock = new Object();

	  // Open the fault.csv file recording fault details
	  String faultCSVName = new File(resultDataDir, "faultInfo.csv").getPath();
	  try {
		faultInfoCSV = new BufferedWriter(new FileWriter(faultCSVName));
		faultInfoCSV.write("name,start,end,duration\n");
	  } catch (IOException e) {
		log.error(e.getMessage());
		System.exit(1);
	  }
	  fault_lock = new Object();

      // Open the aggregated summary.csv file
      String summaryCSVName = new File(resultDataDir, "summary.csv").getPath();
      try {
        summaryCSV = new BufferedWriter(new FileWriter(summaryCSVName));
        summaryCSV.write("ttype,count,percent,mean,max,rollbacks,errors\n");
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("main, writing transaction summary to " + summaryCSVName);

      // Open the histogram.csv file.
      String histogramCSVName = new File(resultDataDir, "histogram.csv").getPath();
      try {
        histogramCSV = new BufferedWriter(new FileWriter(histogramCSVName));
        histogramCSV.write("ttype,edge,numtrans\n");
      } catch (IOException e) {
        log.error(e.getMessage());
        System.exit(1);
      }
      log.info("main, writing transaction histogram to " + histogramCSVName);

	  // Open the txnlog.csv file recording txn log from database
	  String txnlogCSVName = new File(resultDataDir, "txnlog.csv").getPath();
	  try {
		txnlogCSV = new BufferedWriter(new FileWriter(txnlogCSVName));
		txnlogCSV.write("txn_id\n");
	  } catch (IOException e) {
		log.error(e.getMessage());
		System.exit(1);
	  }
	  log.info("main, writing transaction log to {}", txnlogCSVName);

      // Launch the metric collector script if configured
      if (osCollectorScript != null) {
        try {
          osCollector = new OSCollector(getSysVal("osCollectorScript"),
              resultDataDir);
        } catch (IOException e) {
          log.error(e.getMessage());
          System.exit(1);
        }
      }

      log.info("main,");
    }

    /*
     * Even though we don't deal with the report generator in the main
     * java code (the Flask UI does that and it is available on the
     * command line), we consume the property so that it is reported
     * in the logs.
     */
    String reportScript = getSysVal("reportScript");

    /* Initialize the random number generator and report C values. */
    rnd = new jTPCCRandom(loadNuRandCLast);
    log.info("main, ");
    log.info("main, C value for nURandCLast at load: {}", loadNuRandCLast);
    log.info("main, C value for nURandCLast this run: {}", rnd.getNURandCLast());
    log.info("main, ");

    terminal_data = new jTPCCTData[useWarehouses * numTerms];

    /* Create the scheduler. */
    scheduler = new jTPCCScheduler(this);
    scheduler_thread = new Thread(this.scheduler);
    scheduler_thread.start();

    /*
     * Create the SUT and schedule the launch of the SUT threads.
     */
    result_begin = now + rampupMins * 60000;
    result_end = result_begin + runMins * 60000;

    systemUnderTest = new jTPCCSUT(this);
    for (int t = 0; t < numSUTThreads; t++) {
      jTPCCTData sut_launch_tdata;
      sut_launch_tdata = new jTPCCTData();

      /*
       * We abuse the term_w_id to communicate which of the SUT threads to start.
       */
      sut_launch_tdata.term_w_id = t;
      scheduler.at(now + t * sutThreadDelay, jTPCCScheduler.SCHED_SUT_LAUNCH, sut_launch_tdata);
    }

    /*
     * Launch the threads that generate the terminal input data.
     */
    monkeys = new jTPCCMonkey(this);

    /*
     * Create all the Terminal data sets and schedule their launch. We only assign their fixed
     * TERM_W_ID is TERM_D_ID (for stock level transactions) here. Once the scheduler is actually
     * launching them according to their delay, the trained monkeys will fill in real data, send
     * them back into the scheduler queue to the flow to the client threads performing the real DB
     * work.
     */
    for (int t = 0; t < useWarehouses * numTerms; t++) {
      terminal_data[t] = new jTPCCTData();
      terminal_data[t].term_w_id = (t / numTerms) + useWarehouseFrom;
      terminal_data[t].term_d_id = (t % 10) + 1;
      terminal_data[t].trans_type = jTPCCTData.TT_NONE;
      terminal_data[t].trans_due = now + t * terminalDelay;
      terminal_data[t].trans_start = terminal_data[t].trans_due;
      terminal_data[t].trans_end = terminal_data[t].trans_due;
      terminal_data[t].trans_error = false;

      scheduler.at(terminal_data[t].trans_due, jTPCCScheduler.SCHED_TERM_LAUNCH, terminal_data[t]);
    }

    /*
     * Schedule the special events to begin measurement (end of rampup time), to shut down the
     * system and to print messages when the terminals and SUT threads have all been started.
     */
    this.scheduler.at(result_begin, jTPCCScheduler.SCHED_BEGIN, new jTPCCTData());
    this.scheduler.at(result_end, jTPCCScheduler.SCHED_END, new jTPCCTData());
    this.scheduler.at(result_end + 10000, jTPCCScheduler.SCHED_DONE, new jTPCCTData());
    this.scheduler.at(now + (useWarehouses * numTerms) * terminalDelay,
        jTPCCScheduler.SCHED_TERM_LAUNCH_DONE, new jTPCCTData());
    this.scheduler.at(now + numSUTThreads * sutThreadDelay, jTPCCScheduler.SCHED_SUT_LAUNCH_DONE,
        new jTPCCTData());
    if (reportIntervalSecs > 0) {
      this.scheduler.at(now + reportIntervalSecs * 1000, jTPCCScheduler.SCHED_REPORT,
          new jTPCCTData());
    }
    this.scheduler.at(now + resultIntervalSecs * 1000, jTPCCScheduler.SCHED_DUMMY_RESULT, new jTPCCTData());

    try {
      scheduler_thread.join();
      log.info("main, scheduler returned");
    } catch (InterruptedException e) {
      log.error("main, InterruptedException: {}", e.getMessage());
    }

    /*
     * Time to stop input data generation.
     */
    monkeys.terminate();
    log.info("main, all simulated terminals ended");

    /*
     * Stop the SUT.
     */
    systemUnderTest.terminate();
    log.info("main, all SUT threads ended");

    /*
     * Report final transaction statistics.
     */
    monkeys.reportStatistics();

    /*
     * Close the aggregated transaction CSV result
     */
    if (resultCSV != null) {
      try {
        log.info("aggregated transaction result file finished");
        resultCSV.close();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }

    /*
     * Close the summary CSV
     */
    if (summaryCSV != null) {
      try {
        log.info("transaction summary file finished");
        summaryCSV.close();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }

    /*
     * Close the histogram CSV
     */
    if (histogramCSV != null) {
      try {
        log.info("transaction histogram file finished");
        histogramCSV.close();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }

    /*
     * Stop the OS stats collector
     */
    if (osCollector != null) {
      try {
        osCollector.stop();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
      osCollector = null;
      log.info("main, OS Collector stopped");
    }

	// read all txn_id from bmsql_txnlog and write to txnlog.csv
	try {
	  Connection dbConn = DriverManager.getConnection(iConn, iUser, iPassword);
	  dbConn.setAutoCommit(false);
	  PreparedStatement txnlogStmt = dbConn.prepareStatement("SELECT txn_id FROM bmsql_txnlog");
	  ResultSet rs = txnlogStmt.executeQuery();
	  while (rs.next()) {
		long txn_id = rs.getLong("txn_id");
		txnlogCSV.write(txn_id + "\n");
	  }
	  txnlogCSV.flush();
	  txnlogStmt.close();
	  dbConn.close();
	} catch (Exception ex) {
	  log.error("main, Unable to read bmsql_txnlog");
	  log.error("main, {}", ex.getMessage());
	}

	/*
	 * Close the txnlog CSV
	 */
	if (txnlogCSV != null) {
	  try {
		log.info("transaction log file finished");
		txnlogCSV.close();
	  } catch (Exception e) {
		log.error(e.getMessage());
	  }
	}
  }

  public jTPCCApplication getApplication() {
    if (applicationName.equals("Generic"))
      return new AppGeneric();
    if (applicationName.equals("CRDB"))
      return new AppCRDB();
    if (applicationName.equals("PostgreSQLStoredProc"))
      return new AppPostgreSQLStoredProc();
    if (applicationName.equals("OracleStoredProc"))
      return new AppOracleStoredProc();

    return new jTPCCApplication();
  }

  public static void csv_result_write(String line) {
    if (resultCSV != null) {
      synchronized (result_lock) {
        try {
          resultCSV.write(line);
          resultCSV.flush();
        } catch (Exception e) {
        }
      }
    }
  }

  public static void csv_trace_write(String line) {
	if (traceCSV != null) {
	  synchronized (trace_lock) {
		try {
		  traceCSV.write(line);
		  traceCSV.flush();
		} catch (Exception e) {
		}
	  }
	}
  }

  public static void csv_fault_write(String line) {
	if (faultInfoCSV != null) {
	  synchronized (fault_lock) {
		try {
		  faultInfoCSV.write(line);
		  faultInfoCSV.flush();
		} catch (Exception e) {
		}
	  }
	}
  }

  public static void csv_summary_write(String line) {
    if (summaryCSV != null) {
      try {
        summaryCSV.write(line);
      } catch (Exception e) {
      }
    }
  }

  public static void csv_histogram_write(String line) {
    if (histogramCSV != null) {
      try {
        histogramCSV.write(line);
      } catch (Exception e) {
      }
    }
  }

  private void exit() {
    System.exit(0);
  }

  private String getCurrentTime() {
    return jTPCCConfig.dateFormat.format(new java.util.Date());
  }

  private String getFileNameSuffix() {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    return dateFormat.format(new java.util.Date());
  }

  public static void copyFaultFile(String src) throws Exception{
	  File in = new File(src);
	  File out = new File(new File(resultDirName, "data"), "fault.yaml");
	  copyFile(in, out);
  }

  private static void copyFile(File in, File out) throws FileNotFoundException, IOException {
    FileInputStream strIn = new FileInputStream(in);
    FileOutputStream strOut = new FileOutputStream(out);
    byte buf[] = new byte[65536];

    int len = strIn.read(buf);
    while (len > 0) {
      strOut.write(buf, 0, len);
      len = strIn.read(buf);
    }

    strOut.close();
    strIn.close();
  }
}
