package com.github.pgsqlio.benchmarksql.jtpcc;

import org.junit.jupiter.api.Test;

import com.github.pgsqlio.benchmarksql.chaos.ChaosFault;
import com.github.pgsqlio.benchmarksql.chaos.ChaosInjecter;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;
import java.io.StringWriter;

public class InjectTest {

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
		p.setProperty("k8scli", "wy@133.133.135.56");
		p.setProperty("namespace", "oceanbase");
		p.setProperty("iface", "ens6f1");
		p.setProperty("pods", "ref-obzone=obcluster-1-zone1, ref-obzone=obcluster-1-zone1, ref-obzone=obcluster-1-zone1");
		p.setProperty("leader", "ref-obzone=obcluster-1-zone1");
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
			HashMap<String, Object> describe = yaml.load(input);
			StringWriter writer = new StringWriter();
			DumperOptions options = new DumperOptions();
			options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK); 
			options.setPrettyFlow(true); 
			Yaml yamlDumper = new Yaml(options);
			yamlDumper.dump(describe, writer);
			String yamlString = writer.toString();

			String expected = String.format("apiVersion: chaos-mesh.org/v1alpha1\nkind: PodChaos\nmetadata:\n  name: fail-pod-by-labels\n  namespace: chaos-testing\nspec:\n  action: pod-kill\n  mode: one\n  selector:\n    namespaces:\n    - oceanbase\n    labelSelectors:\n      ref-obzone: obcluster-1-zone1\n");
			assertEquals(expected, yamlString);

		} catch (Exception e) {
			assertTrue(false, e.getMessage());
			e.printStackTrace();
		}
	}
}
