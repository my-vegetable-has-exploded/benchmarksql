package com.github.pgsqlio.benchmarksql.jtpcc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;
import java.util.ArrayList;

public class SystemConfig {
	private static Logger logger = LogManager.getLogger(SystemConfig.class);

	public String k8scli;
	public String namespace;
	public String iface;
	public ArrayList<String> pods;
	public String leader;
	public int serverport;
	public ArrayList<String> faults;
	// in mins
	public int faultTime;

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
		k8scli = getProp(p, "k8scli");
		namespace = getProp(p, "namespace");
		iface = getProp(p, "iface");
		leader = getProp(p, "leader");
		serverport = Integer.parseInt(getProp(p, "serverport"));
		String podsStr = getProp(p, "pods");
		pods = new ArrayList<String>();
		for (String pod : podsStr.split(",")){
			pods.add(pod.strip());
		}
		String faultsStr = getProp(p, "faults");
		faults = new ArrayList<String>();
		for (String fault : faultsStr.split(",")){
			faults.add(fault.strip());
		}
		faultTime = Integer.parseInt(getProp(p, "faulttime", "10"));
	}
}
