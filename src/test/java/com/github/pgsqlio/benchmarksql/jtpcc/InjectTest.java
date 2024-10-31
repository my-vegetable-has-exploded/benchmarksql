package com.github.pgsqlio.benchmarksql.jtpcc;

import org.junit.jupiter.api.Test;

import com.github.pgsqlio.benchmarksql.chaos.ChaosFault;
import com.github.pgsqlio.benchmarksql.chaos.ChaosInjecter;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import org.yaml.snakeyaml.Yaml;

public class InjectTest {

	class ChaosBlade {
		private String apiVersion;
		private String kind;
		private Map<String, Object> metadata;
		private Spec spec;

		// getters and setters
	}

	class Spec {
		private List<Experiment> experiments;

		// getters and setters
	}

	class Experiment {
		private String scope;
		private String target;
		private String action;
		private String desc;
		private List<Matcher> matchers;

		// getters and setters
	}

	class Matcher {
		private String name;
		private List<String> value;

		// getters and setters
	}

	@Test
	public void testInject() {
		// example properties
		// # k8scli user@ip
		// k8scli: "root@133.133.135.56"
		// namespace: "oceanbase"
		// iface: "ens6f1"
		// pods: "zone1, zone2, zone3"
		// leader: "zone1"
		// # port of db server
		// serverport: 2883
		// faults: "leader_fail.yaml"

		Properties p = new Properties();
		p.setProperty("k8scli", "root@133.133.135.56");
		p.setProperty("namespace", "oceanbase");
		p.setProperty("iface", "ens6f1");
		p.setProperty("pods", "zone1, zone2, zone3");
		p.setProperty("leader", "zone1");
		p.setProperty("serverport", "2883");
		p.setProperty("faults", "leader_fail.yaml");

		// println current path
		ChaosInjecter injecter = ChaosInjecter.getInstance(null, "src/main/resources/FaultTemplates/",
				"src/main/resources/faults/");

		SystemConfig config = new SystemConfig(p);
		try {
			ChaosFault fault = injecter.initialFault(config, "leader_fail.yaml");
			String faultPath = fault.file;
			// read yaml file
			InputStream input = new FileInputStream(faultPath);
			Yaml yaml = new Yaml();
			ChaosBlade describe = yaml.load(input);
			assertEquals("zone1", describe.spec.experiments.get(0).matchers.get(0).value.get(0));
			assertEquals("oceanbase", describe.spec.experiments.get(0).matchers.get(1).value.get(0));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
