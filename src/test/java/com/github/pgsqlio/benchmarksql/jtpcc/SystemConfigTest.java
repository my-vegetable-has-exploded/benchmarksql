package com.github.pgsqlio.benchmarksql.jtpcc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;

public class SystemConfigTest {

	@Test
	public void testSystemConfig() {
		// example parameters
		// sys.k8scli: "root@133.133.135.56"
		// sys.namespace: "oceanbase"
		// sys.iface: "ens6f1"
		// sys.pods: "zone1, zone2, zone3"
		// sys.leader: "zone1"
		// # port of db server
		// sys.serverport: 2883
		// sys.faults: "leader_fail.yaml"
		Properties p = new Properties();
		p.setProperty("sys.k8scli", "root@133.133.135.56");
		p.setProperty("sys.namespace", "oceanbase");
		p.setProperty("sys.iface", "ens6f1");
		p.setProperty("sys.pods", "zone1, zone2, zone3");
		p.setProperty("sys.leader", "zone1");
		p.setProperty("sys.serverport", "2883");
		p.setProperty("sys.faults", "leader_fail.yaml");

		SystemConfig config = new SystemConfig(p);
		assertEquals("root@133.133.135.56", config.k8scli);
		assertEquals("oceanbase", config.namespace);
		assertEquals("ens6f1", config.confs.get("iface"));
		assertEquals("zone1", config.leader);
		assertEquals("2883", config.serverport);
		assertEquals(3, config.pods.size());
		assertEquals("zone1", config.pods.get(0));
		assertEquals("zone2", config.pods.get(1));
		assertEquals("zone3", config.pods.get(2));
		assertEquals(1, config.faults.size());
		assertEquals("leader_fail.yaml", config.faults.get(0));
	}
}
