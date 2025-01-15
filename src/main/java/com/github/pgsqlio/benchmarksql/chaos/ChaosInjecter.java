package com.github.pgsqlio.benchmarksql.chaos;

import com.github.pgsqlio.benchmarksql.jtpcc.SystemConfig;
import com.github.pgsqlio.benchmarksql.jtpcc.jTPCC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

// ChaosInjecter - Inject chaos faults
public class ChaosInjecter {
	private jTPCC gdata;
	Logger logger = LogManager.getLogger(ChaosInjecter.class);
	private static ChaosInjecter instance;
	private final String templatePath;
	private final String faultPath;
	ChaosClient client = new ChaosClient();

	private ChaosInjecter(jTPCC gdata) {
		this.gdata = gdata;
		this.templatePath = "./FaultTemplates/";
		this.faultPath = "./faults/";
	}

	private ChaosInjecter(jTPCC gdata, String templatePath, String faultPath) {
		this.gdata = gdata;
		this.templatePath = templatePath;
		this.faultPath = faultPath;
	}

	public static ChaosInjecter getInstance(jTPCC gdata) {
		if (instance == null) {
			synchronized (ChaosInjecter.class) {
				if (instance == null) {
					instance = new ChaosInjecter(gdata);
				}
			}
		}
		return instance;
	}

	public static ChaosInjecter getInstance(jTPCC gdata, String templatePath, String faultPath) {
		if (instance == null) {
			synchronized (ChaosInjecter.class) {
				if (instance == null) {
					instance = new ChaosInjecter(gdata, templatePath, faultPath);
				}
			}
		}
		return instance;
	}

	public static void main(String[] args) throws Exception {
		ChaosInjecter injecter = ChaosInjecter.getInstance(null);
		injecter.inject();
		Thread.sleep(40000);
	}

	private static void findPlaceholders(Object data, ArrayList<String> placeholders) {
		if (data instanceof Map) {
			Map<?, ?> map = (Map<?, ?>) data;
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				findPlaceholders(entry.getValue(), placeholders);
			}
		} else if (data instanceof List) {
			List<?> list = (List<?>) data;
			for (Object item : list) {
				findPlaceholders(item, placeholders);
			}
		} else if (data instanceof String) {
			String value = (String) data;
			if (value.startsWith("$")) {
				placeholders.add(value);
			}
		}
	}

	private static void replacePlaceholders(Object data, HashMap<String, Object> replacements) {
		if (data instanceof Map) {
			Map<Object, Object> map = (Map<Object, Object>) data;
			for (Map.Entry<Object, Object> entry : map.entrySet()) {
				Object value = entry.getValue();
				if (value instanceof String) {
					entry.setValue(getReplacement((String) value, replacements));
				} else {
					replacePlaceholders(value, replacements);
				}
			}
		} else if (data instanceof List) {
			List<Object> list = (List<Object>) data;
			for (int i = 0; i < list.size(); i++) {
				Object item = list.get(i);
				if (item instanceof String) {
					list.set(i, getReplacement((String) item, replacements));
				} else {
					replacePlaceholders(item, replacements);
				}
			}
		}
	}

	private static Object getReplacement(String value, HashMap<String, Object> replacements) {
		if (value.startsWith("$")) {
			Object replacement1 = replacements.get(value);
			if (replacement1 != null) {
				return replacement1;
			}
			// map $PODNAME to podname
			Object replacement2 = replacements.get(value.substring(1).toLowerCase());
			if (replacement2 != null) {
				return replacement2;
			}
		}
		return value;
	}

	public HashMap<String, Object> computeReplacements(SystemConfig config, ArrayList<String> placeholders) throws Exception {
		HashMap<String, Object> replacements = new HashMap<String, Object>();
		for (String placeholder : placeholders) {
			if (placeholder.equals("$LEADER")) {
				replacements.put("$LEADER", configParse(config.leader));
			} else if (placeholder.equals("$RANDOMPOD")) {
				int podsSize = config.pods.size();
				int randomPodIndex = (int) (Math.random() * podsSize);
				String randomPod = config.pods.get(randomPodIndex);
				replacements.put("$RANDOMPOD", configParse(randomPod));
			} else if (placeholder.equals("$IFACE")) {
				replacements.put("$IFACE", configParse(config.iface));
			}
		}
		// extend replacements systemconfig with config.confs
		for (Map.Entry<String, String> entry : config.confs.entrySet()) {
			replacements.put("$"+entry.getKey().toUpperCase(), configParse(entry.getValue()));
		}
		// compute random pod, shuffle pods and insert all with index
		java.util.Random random = new java.util.Random();
		List<Object> pods = new ArrayList<Object>();
		for (String pod : config.pods) {
			pods.add(configParse(pod));
		}
		java.util.Collections.shuffle(pods, random);
		for (int i = 0; i < pods.size(); i++) {
			replacements.put("$RANDOMPOD" + (i + 1), pods.get(i));
		}
		return replacements;
	}

	public static Object configParse(String conf) {
		// if conf is 'label=value', return a map with key 'label' and value 'value'
		if (conf.contains("=")) {
			String[] parts = conf.split("=");
			HashMap<String, Object> map = new HashMap<String, Object>();
			map.put(parts[0], parts[1]);
			return map;
		}
		return conf;
	}

	public int durationParse(String duration) throws Exception {
		// parse duration and return milliseconds
		if (duration.endsWith("s")) {
			return Integer.parseInt(duration.substring(0, duration.length() - 1)) * 1000;
		} else if (duration.endsWith("m")) {
			return Integer.parseInt(duration.substring(0, duration.length() - 1)) * 60 * 1000;
		} else if (duration.endsWith("min")) {
			return Integer.parseInt(duration.substring(0, duration.length() - 3)) * 60 * 1000;
		} else if (duration.endsWith("ms")) {
			return Integer.parseInt(duration.substring(0, duration.length() - 2));
		} else {
			throw new Exception("invalid duration: " + duration);
		}
	}

	public ChaosFault initialFault(SystemConfig config, String faultName) throws Exception {
		// read fault description from faultName
		String faultTemplateFile = templatePath + faultName;
		// read fault template
		InputStream faultTemplate = new FileInputStream(faultTemplateFile);
		// parse fault template
		Yaml yaml = new Yaml();
		HashMap<String, Object> fault = yaml.load(faultTemplate);
		int duration = durationParse((String) fault.get("duration"));
		// find placeholders
		ArrayList<String> placeholders = new ArrayList<String>();
		findPlaceholders(fault, placeholders);
		// compute replacements
		HashMap<String, Object> replacements = computeReplacements(config, placeholders);
		// replace placeholders
		replacePlaceholders(fault, replacements);

		String chaosTemplateFile = templatePath + "template/" + fault.get("template");
		InputStream chaosTemplate = new FileInputStream(chaosTemplateFile);
		HashMap<String, Object> chaos = yaml.load(chaosTemplate);
		// // find placeholders
		// ArrayList<String> chaosPlaceholders = new ArrayList<String>();
		// findPlaceholders(chaos, chaosPlaceholders);
		replacePlaceholders(chaos, fault);
		// handle rest placeholders by replacements
		replacePlaceholders(chaos, replacements);

		String chaosData = yaml.dump(chaos);
		String chaosPath = faultPath + faultName;
		// show chaosData in logger
		logger.info("chaosData: {}", chaosData);
		OutputStream chaosOutput = new FileOutputStream(chaosPath);
		chaosOutput.write(chaosData.getBytes());
		chaosOutput.close();
		return new ChaosFault(config.k8scli, chaosPath, duration);
	}

	public void inject() throws Exception {
		ArrayList<ChaosFault> faults = new ArrayList<ChaosFault>();
		for (String faultName : gdata.sysConfig.faults) {
			faults.add(initialFault(gdata.sysConfig, faultName));
		}
		for (ChaosFault fault : faults) {
			inject(fault);
		}
	}

	public void inject(ChaosFault fault) {
		logger.info("injecting fault: {}, duration: {}", fault.file, fault.duration);
		client.inject(fault);
	}
}
