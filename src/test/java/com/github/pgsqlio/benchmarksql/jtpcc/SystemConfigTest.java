package com.github.pgsqlio.benchmarksql.jtpcc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;

public class SystemConfigTest {

	@Test
	public void testSystemConfig() {
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
		// sys.compute.pods: "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2"
		// sys.volumePath:/home/admin/data-file
		// sys.faults: "leader_fail.yaml"
		Properties p = new Properties();

		p.setProperty("sys.k8scli", "root@133.133.135.56");
		p.setProperty("sys.namespace", "oceanbase");
		p.setProperty("sys.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		p.setProperty("sys.leaderzone", "zone1");
		p.setProperty("sys.zones", "zone1,zone2,zone3");
		p.setProperty("sys.zone1.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr");
		p.setProperty("sys.zone2.pods", "obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd");
		p.setProperty("sys.zone3.pods", "obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		p.setProperty("sys.storage.pods", "obcluster-1-zone1-f4zc55,obcluster-1-zone1-vlqshr,obcluster-1-zone2-st8k4g,obcluster-1-zone2-zmrqjd,obcluster-1-zone3-4jqrvf,obcluster-1-zone3-pvfsh2");
		p.setProperty("sys.volumePath", "/home/admin/data-file");
		p.setProperty("sys.faults", "leader_fail.yaml");

		SystemConfig config = new SystemConfig(p);
		assertEquals("root@133.133.135.56", config.k8scli);
		assertEquals("oceanbase", config.namespace);

	}
}
