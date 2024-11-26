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
		// sys.k8scli: "root@133.133.135.56"
		// sys.namespace: "oceanbase"
		// sys.iface: "ens6f1"
		// sys.pods: "zone1, zone2, zone3"
		// sys.leader: "zone1"
		// # port of db server
		// sys.serverport: 2883
		// sys.faults: "leader_fail.yaml"

		Properties p = new Properties();
		p.setProperty("sys.k8scli", "wy@133.133.135.56");
		p.setProperty("sys.namespace", "oceanbase");
		p.setProperty("sys.iface", "ens6f1");
		p.setProperty("sys.pods", "ref-obzone=obcluster-1-zone1, ref-obzone=obcluster-1-zone1, ref-obzone=obcluster-1-zone1");
		p.setProperty("sys.leader", "ref-obzone=obcluster-1-zone1");
		p.setProperty("sys.serverport", "2883");
		p.setProperty("sys.faults", "leader_fail.yaml");

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
			e.printStackTrace();
			assertEquals("", e.getMessage());
		}
	}
}
