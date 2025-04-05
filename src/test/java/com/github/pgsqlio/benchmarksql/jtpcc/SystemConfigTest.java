package com.github.pgsqlio.benchmarksql.jtpcc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;

public class SystemConfigTest {

	@Test
	public void testSystemConfig() {
		// example properties
		// # k8scli user@ip
		// sys.k8scli: "wy@133.133.135.56"
		// sys.components: "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver"
		// sys.leaderzone: "zone1"
		// sys.zones: "zone1,zone2,zone3"
		// sys.zone1.components: "133.133.135.156:dmserver"
		// sys.zone2.components: "133.133.135.157:dmserver"
		// sys.zone3.components: "133.133.135.158:dmserver"
		// sys.storage.components: "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver"
		// sys.volumePath:/home
		// sys.storage.volumePath:/home
		// sys.faults: "leader_fail.yaml"

        //  convert example properties to codes     
		Properties p = new Properties();

		p.setProperty("sys.k8scli", "wy@133.133.135.56");
        p.setProperty("sys.components", "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver");
        p.setProperty("sys.leaderzone", "zone1");
        p.setProperty("sys.zones", "zone1,zone2,zone3");
        p.setProperty("sys.zone1.components",  "133.133.135.156:dmserver");
        p.setProperty("sys.zone2.components",  "133.133.135.157:dmserver");
        p.setProperty("sys.zone3.components",  "133.133.135.158:dmserver");
        p.setProperty("sys.storage.components",  "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver");
        p.setProperty("sys.storage.volumePath", "/home");
        p.setProperty("sys.volumePath", "/home");
        p.setProperty("sys.faults", "leader_fail.yaml");

		SystemConfig config = new SystemConfig(p);
		assertEquals("wy@133.133.135.56", config.k8scli);
        // assert  components in config.components
        assertEquals(3, config.components.size());
        
	}
}
