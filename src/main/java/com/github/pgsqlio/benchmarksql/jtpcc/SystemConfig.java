package com.github.pgsqlio.benchmarksql.jtpcc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Enumeration;

public class SystemConfig {
	private static Logger logger = LogManager.getLogger(SystemConfig.class);

	public String k8scli;
	public String namespace;
	public String iface;
	public ArrayList<String> pods;
	public String leader;
	public String serverport;
	public ArrayList<String> faults;
	public String volumePath;
	// in mins
	public int faultTime;
	public HashMap<String, String> confs = new HashMap<>();

	private String getProp(Properties p, String pName) {
		String prop = p.getProperty(pName);
		logger.info("system config, {}={}", pName, prop);
		return (prop);
	}

	private String getProp(Properties p, String pName, String defVal) {
		String prop = p.getProperty(pName);
		if (prop == null)
			prop = defVal;
		logger.info("system config, {}={}", pName, prop);
		return (prop);
	}

	public SystemConfig(Properties p) {
		// get the properties
		k8scli = getProp(p, "sys.k8scli");
		namespace = getProp(p, "sys.namespace");
		iface = getProp(p, "sys.iface");
		leader = getProp(p, "sys.leader");
		serverport = getProp(p, "sys.serverport");
		volumePath = getProp(p, "sys.volumePath");
		String podsStr = getProp(p, "sys.pods");
		pods = new ArrayList<String>();
		for (String pod : podsStr.split(",")){
			pods.add(pod.strip());
		}
		String faultsStr = getProp(p, "sys.faults");
		faults = new ArrayList<String>();
		for (String fault : faultsStr.split(",")){
			faults.add(fault.strip());
		}
		faultTime = Integer.parseInt(getProp(p, "sys.faulttime", "10"));

		// add all properties start with "sys." to confs, and remove "sys." prefix
		Enumeration<?> propertyNames = p.propertyNames();
		while (propertyNames.hasMoreElements()) {
			String key = (String) propertyNames.nextElement();
			if (key.startsWith("sys.")) {
				String value = getProp(p, key);
				System.err.println(key.substring(4)+" "+value);
				confs.put(key.substring(4), value);
				logger.info("system config, {}={}", key.substring(4), value);
			}
		}
	}
}
