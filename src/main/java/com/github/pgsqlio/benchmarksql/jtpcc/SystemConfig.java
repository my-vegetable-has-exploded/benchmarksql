package com.github.pgsqlio.benchmarksql.jtpcc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Enumeration;

public class SystemConfig {
	private static Logger logger = LogManager.getLogger(SystemConfig.class);

	public Properties p;
	public String k8scli;
	public String namespace;
	public ArrayList<String> pods = new ArrayList<String>();
	public String leaderzone;
	public ArrayList<String> zones = new ArrayList<String>();
	public HashMap<String, ArrayList<String>> zonePods = new HashMap<String, ArrayList<String>>();
	public ArrayList<String> faults = new ArrayList<String>();
	public String volumePath;
	public HashMap<String, ArrayList<String>> scopesCache = new HashMap<String, ArrayList<String>>();
	// in mins
	public int faultTime;
	public HashMap<String, String> confs = new HashMap<>();

	public String getProp(Properties p, String pName) {
		String prop = p.getProperty(pName);
		if (prop != null) {
			logger.info("system config, {}={}", pName, prop);
		}else {
			logger.info("system config, {} is not set", pName);
		}
		return (prop);
	}

	public String getProp(Properties p, String pName, String defVal) {
		String prop = p.getProperty(pName);
		if (prop == null)
			prop = defVal;
		logger.info("system config, {}={}", pName, prop);
		return (prop);
	}

	public SystemConfig(Properties p) {
		this.p = p;
		// get the properties
		k8scli = getProp(p, "sys.k8scli");
		namespace = getProp(p, "sys.namespace");
		volumePath = getProp(p, "sys.volumePath");
		String podsStr = getProp(p, "sys.pods");
		for (String pod : podsStr.split(",")) {
			pods.add(pod.strip());
		}
		String zonesStr = getProp(p, "sys.zones");
		if (zonesStr != null) {
			for (String zone : zonesStr.split(",")) {
				zones.add(zone.strip());
				String podsInZoneString = getProp(p, "sys." + zone.strip() + ".pods");
				logger.info("system config, {}.pods={}", zone.strip(), podsInZoneString);
				ArrayList<String> podsInZone = new ArrayList<String>();
				for (String pod : podsInZoneString.split(",")) {
					podsInZone.add(pod.strip());
				}
				zonePods.put(zone.strip(), podsInZone);
			}
		}
		leaderzone = getProp(p, "sys.leaderzone");
		if (leaderzone != null) {
			leaderzone = leaderzone.strip();
		}

		String faultsStr = getProp(p, "sys.faults");
		for (String fault : faultsStr.split(",")) {
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
