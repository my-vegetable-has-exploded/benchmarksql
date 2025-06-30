package com.github.pgsqlio.benchmarksql.jtpcc;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SystemConfig {
	private static Logger logger = LogManager.getLogger(SystemConfig.class);

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

	public String storagePods;
	public String computePods;
	public String testPods;

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
				confs.put(key.substring(4), value);
				logger.info("system config, {}={}", key.substring(4), value);
			}
		}

		storagePods = getProp(p, "sys.storage.pods");
		if (storagePods != null) {
			storagePods = storagePods.strip();
		}
		computePods = getProp(p, "sys.compute.pods");
		if (computePods != null) {
			computePods = computePods.strip();
		}
		testPods = getProp(p, "sys.test.pods");
		if (testPods != null) {
			testPods = testPods.strip();
		}

	}

	String getValFromYaml(HashMap<String, Object> yamlMap, String key, String defVal) {
		Object val = yamlMap.get(key);
		String result = val != null ? val.toString() : defVal;
		logger.info("system config, {}={}", key, result);
		return result;
	}

	String getValFromYaml(HashMap<String, Object> yamlMap, String key) {
		Object val = yamlMap.get(key);
		String result = val != null ? val.toString() : null;
		logger.info("system config, {}={}", key, result);
		return result;
	}

	public SystemConfig(HashMap<String, Object> yamlMap) {
		// get the properties
		k8scli = getValFromYaml(yamlMap, "sys.k8scli");
		namespace = getValFromYaml(yamlMap, "sys.namespace");
		volumePath = getValFromYaml(yamlMap, "sys.volumePath");

		String podsStr = getValFromYaml(yamlMap, "sys.pods");
		if (podsStr != null) {
			for (String pod : podsStr.split(",")) {
				pods.add(pod.strip());
			}
		}

		String zonesStr = getValFromYaml(yamlMap, "sys.zones");
		if (zonesStr != null) {
			for (String zone : zonesStr.split(",")) {
				zones.add(zone.strip());
			}
		}

		for (String zone : zones) {
			String podsInZoneString = getValFromYaml(yamlMap, "sys." + zone + ".pods");
			ArrayList<String> podsInZone = new ArrayList<>();
			if (podsInZoneString != null) {
				for (String pod : podsInZoneString.split(",")) {
					podsInZone.add(pod.strip());
				}
			}
			zonePods.put(zone, podsInZone);
		}

		leaderzone = getValFromYaml(yamlMap, "sys.leaderzone");

		String faultsStr = getValFromYaml(yamlMap, "sys.faults");
		if (faultsStr != null) {
			for (String fault : faultsStr.split(",")) {
				faults.add(fault.strip());
			}
		}

		String faultTimeStr = getValFromYaml(yamlMap, "sys.faulttime");
		if (faultTimeStr != null) {
			faultTime = Integer.parseInt(faultTimeStr);
		} else {
			faultTime = 10; // default
		}

		// populate confs map
		for (String key : yamlMap.keySet()) {
			String valStr = getValFromYaml(yamlMap, key);
			if (key.startsWith("sys.")) {
				confs.put(key.substring(4), valStr);
			}
		}

		storagePods = getValFromYaml(yamlMap, "sys.storage.pods");
		computePods = getValFromYaml(yamlMap, "sys.compute.pods");
		testPods = getValFromYaml(yamlMap, "sys.test.pods");
	}

}
