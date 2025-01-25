package com.github.pgsqlio.benchmarksql.jtpcc;

import org.junit.jupiter.api.Test;

import com.github.pgsqlio.benchmarksql.chaos.ChaosFault;
import com.github.pgsqlio.benchmarksql.chaos.ChaosInjecter;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;
import java.io.StringWriter;

public class InjectTest {

	@Test
	public void testInject() throws Exception {
		// example properties
		// # k8scli user@ip
		// sys.k8scli: "root@133.133.135.56"
		// sys.namespace: "oceanbase"
		// sys.pods: "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2"
		// sys.leaderzone: "zone1"
		// sys.zones: "zone1,zone2,zone3"
		// sys.zone1.pods: "obcluster-1-zone1-f4zc55,obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr"
		// sys.zone2.pods: "obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd"
		// sys.zone3.pods: "obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2"
		// sys.storage.pods: "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2"
		// sys.faults: "leader_fail.yaml"

		Properties p = new Properties();
		p.setProperty("sys.k8scli", "wy@133.133.135.56");
		p.setProperty("sys.namespace", "oceanbase");
		p.setProperty("sys.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		p.setProperty("sys.leaderzone", "zone1");
		p.setProperty("sys.zones", "zone1,zone2,zone3");
		p.setProperty("sys.zone1.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr");
		p.setProperty("sys.zone2.pods", "obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd");
		p.setProperty("sys.zone3.pods", "obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		p.setProperty("sys.storage.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		p.setProperty("sys.test.pods", "obcluster-1-zone1-f4zc55");
		p.setProperty("sys.faults", "leader_fail.yaml");

		// println current path
		ChaosInjecter injecter = ChaosInjecter.getInstance(null, "src/main/resources/FaultTemplates/",
				"src/main/resources/faults/");

		SystemConfig config = new SystemConfig(p);
		try {
			List<String> leader_zone_all = injecter.generateScope(config, "$zone.leader-storage-0");
			assertEquals(leader_zone_all.size(), 2);
			assertEquals(leader_zone_all.get(0), "obcluster-1-zone1-f4zc55");
			assertEquals(leader_zone_all.get(1), "obcluster-1-zone1-vlqshr");

			List<String> leader_zone_one = injecter.generateScope(config, "$zone.leader-storage-1");
			assertEquals(leader_zone_one.size(), 1);
			assertTrue(leader_zone_one.get(0).equals("obcluster-1-zone1-f4zc55") || leader_zone_one.get(0).equals("obcluster-1-zone1-vlqshr"));

			List<String> follower_zone1List = injecter.generateScope(config, "$zone.follower.1-storage-1");
			assertEquals(follower_zone1List.size(), 1);
			assertTrue(follower_zone1List.get(0).equals("obcluster-1-zone2-st8k4g") || follower_zone1List.get(0).equals("obcluster-1-zone2-zmrqjd") || follower_zone1List.get(0).equals("obcluster-1-zone3-4jqrvf") || follower_zone1List.get(0).equals("obcluster-1-zone3-pvfsh2"));

			List<String> testRoleList = injecter.generateScope(config, "$zone.leader-test-0");
			assertEquals(testRoleList.size(), 1);
			assertEquals(testRoleList.get(0), "obcluster-1-zone1-f4zc55");

			ChaosFault fault = injecter.initialFault(config, "leader_zone_all_fail.yaml");
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

			String expected = "apiVersion: chaos-mesh.org/v1alpha1"+
			"\nkind: PodChaos"+
			"\nmetadata:"+
			"\n  name: fail-pod"+
			"\n  namespace: chaos-testing"+
			"\nspec:"+
			"\n  action: pod-failure"+
			"\n  mode: all"+
			"\n  duration: 0s"+
			"\n  selector:"+
			"\n    pods:"+
			"\n      oceanbase:"+
			"\n      - obcluster-1-zone1-f4zc55"+
			"\n      - obcluster-1-zone1-vlqshr\n";
			assertEquals(expected, yamlString);

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		final Properties fp = new Properties();
		fp.setProperty("sys.k8scli", "wy@133.133.135.56");
		fp.setProperty("sys.namespace", "oceanbase");
		fp.setProperty("sys.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		fp.setProperty("sys.zones", "zone1,zone2,zone3");
		fp.setProperty("sys.zone1.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr");
		fp.setProperty("sys.zone2.pods", "obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd");
		fp.setProperty("sys.zone3.pods", "obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		fp.setProperty("sys.storage.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		fp.setProperty("sys.test.pods", "obcluster-1-zone3-pvfsh2");
		fp.setProperty("sys.faults", "leader_fail.yaml");
		final SystemConfig fconfig = new SystemConfig(fp);
		// assert throws exception for $zone.leader-storage-0, leader zone not set
		Exception exception = assertThrows(Exception.class, () -> {
			injecter.generateScope(fconfig, "$zone.leader-storage-0");
		});
		assertEquals("leader zone not set", exception.getMessage());

	}
}
