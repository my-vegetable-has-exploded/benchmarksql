package com.github.pgsqlio.benchmarksql.chaos;

import com.github.pgsqlio.benchmarksql.jtpcc.SystemConfig;
import com.github.pgsqlio.benchmarksql.jtpcc.jTPCC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

	private static void replacePlaceholders(Object data, HashMap<String, String> replacements) {
		if (data instanceof Map) {
			Map<?, ?> map = (Map<?, ?>) data;
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				replacePlaceholders(entry.getValue(), replacements);
			}
		} else if (data instanceof List) {
			List<?> list = (List<?>) data;
			for (Object item : list) {
				replacePlaceholders(item, replacements);
			}
		} else if (data instanceof String) {
			String value = (String) data;
			if (value.startsWith("$")) {
				String replacement = replacements.get(value);
				if (replacement != null) {
					// TODO: replace the value
					data = replacement;
				}
			}
		}
	}

	public HashMap<String, String> computeReplacements(SystemConfig config, ArrayList<String> placeholders) {
		HashMap<String, String> replacements = new HashMap();
		for(String placeholder: placeholders) {
			if(placeholder.equals("$LEADER")) {
				replacements.put("$LEADER", config.leader);
			} else if(placeholder.equals("$RANDOMPOD")) {
				int podsSize = config.pods.size();
				int randomPodIndex = (int) (Math.random() * podsSize);
				String randomPod = config.pods.get(randomPodIndex);
				replacements.put("$RANDOMPOD", randomPod);
			} else if(placeholder.equals("$IFACE")) {
				replacements.put("$IFACE", config.iface);
			} else if(placeholder.equals("$SERVERPORT")) {
				replacements.put("$SERVERPORT", Integer.toString(config.serverport));
			}
		}
		return replacements;
	}

	public ChaosFault initialFault(String faultName) throws Exception {
		// read fault description from faultName
		String faultTemplateFile = templatePath + faultName;
		// read fault template
		InputStream faultTemplate = new FileInputStream(faultTemplateFile);
		// parse fault template
		Yaml yaml = new Yaml();
		HashMap<String, String> fault = yaml.load(faultTemplate);
		int duration = Integer.parseInt(fault.get("duration"));
		// find placeholders
		ArrayList<String> placeholders = new ArrayList<String>();
		findPlaceholders(fault, placeholders);
		// compute replacements
		SystemConfig config = gdata.sysConfig;
		HashMap<String, String> replacements = computeReplacements(config, placeholders);
		// replace placeholders
		replacePlaceholders(fault, replacements);

		String chaosTemplateFile = templatePath + "template/" + fault.get("template");
		InputStream chaosTemplate = new FileInputStream(chaosTemplateFile);
		HashMap<String, Object> chaos = yaml.load(chaosTemplate);
		// find placeholders
		ArrayList<String> chaosPlaceholders = new ArrayList<String>();
		findPlaceholders(chaos, chaosPlaceholders);
		replacePlaceholders(chaos, fault);	

		String chaosData = yaml.dump(chaos);
		String chaosPath = faultPath + faultName;
		OutputStream chaosOutput = new FileOutputStream(chaosPath);
		chaosOutput.write(chaosData.getBytes());
		chaosOutput.close();
		return new ChaosFault(config.k8scli, chaosPath, duration);
	}

	public void inject() throws Exception{
		ArrayList<ChaosFault> faults = new ArrayList<ChaosFault>();
		for (String faultName : gdata.sysConfig.faults) {
			faults.add(initialFault(faultName));
		}
		for (ChaosFault fault : faults) {
			inject(fault);
		}
	}

	public void inject(ChaosFault fault) {
		ChaosClient client = new ChaosClient();
		client.inject(fault);		
	}
}
