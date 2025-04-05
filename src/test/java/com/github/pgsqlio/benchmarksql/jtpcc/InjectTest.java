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
        // sys.k8scli: "wy@133.133.135.56"
        // sys.components:
        // "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver"
        // sys.leaderzone: "zone1"
        // sys.zones: "zone1,zone2,zone3"
        // sys.zone1.components: "133.133.135.156:dmserver"
        // sys.zone2.components: "133.133.135.157:dmserver"
        // sys.zone3.components: "133.133.135.158:dmserver"
        // sys.storage.components:
        // "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver"
        // sys.volumePath:/home
        // sys.device: em1
        // sys.storage.volumePath:/home
        // sys.faults: "leader_fail.yaml"

        Properties p = new Properties();
        p.setProperty("sys.k8scli", "wy@133.133.135.56");
        p.setProperty("sys.components", "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver");
        p.setProperty("sys.leaderzone", "zone1");
        p.setProperty("sys.zones", "zone1,zone2,zone3");
        p.setProperty("sys.zone1.components", "133.133.135.156:dmserver");
        p.setProperty("sys.zone2.components", "133.133.135.157:dmserver");
        p.setProperty("sys.zone3.components", "133.133.135.158:dmserver");
        p.setProperty("sys.storage.components",
                "133.133.135.156:dmserver,133.133.135.157:dmserver,133.133.135.158:dmserver");
        p.setProperty("sys.storage.volumePath", "/home");
        p.setProperty("sys.volumePath", "/home");
        p.setProperty("sys.device", "em1");
        p.setProperty("sys.test.components", "133.133.135.158:dmserver");
        p.setProperty("sys.faults", "leader_fail.yaml");

        // println current path
        ChaosInjecter injecter = ChaosInjecter.getInstance(null, "src/main/resources/FaultTemplates/",
                "src/main/resources/faults/");

        SystemConfig config = new SystemConfig(p);
        try {
            List<String> leader_zone_all = injecter.generateScope(config, "$zone.leader-storage-0");
            assertEquals(leader_zone_all.size(), 1);
            assertEquals(leader_zone_all.get(0), "133.133.135.156:dmserver");

            List<String> follower_zone1List = injecter.generateScope(config, "$zone.follower.1-storage-0");
            assertEquals(follower_zone1List.size(), 1);
            assertTrue(follower_zone1List.get(0).equals("133.133.135.157:dmserver")
                    || follower_zone1List.get(0).equals("133.133.135.158:dmserver"));

            List<String> allList = injecter.generateScope(config, "$storage-0");
            assertEquals(allList.size(), 3);

            List<String> testRoleList = injecter.generateScope(config, "$test-0");
            assertEquals(testRoleList.size(), 1);
            assertEquals(testRoleList.get(0), "133.133.135.158:dmserver");

            ChaosFault fault = injecter.initialFault(config, "leader_zone_storage_all_fail.yaml");
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
            input.close();

            String expected = "kind: PhysicalMachineChaos" +
                    "\napiVersion: chaos-mesh.org/v1alpha1" +
                    "\nmetadata:" +
                    "\n  namespace: chaos-testing" +
                    "\n  name: kill-process" +
                    "\nspec:" +
                    "\n  action: process" +
                    "\n  address:" +
                    "\n  - http://133.133.135.156:31767" +
                    "\n  mode: all" +
                    "\n  process:" +
                    "\n    process: dmserver" +
                    "\n    signal: 9" +
                    "\n  duration: 120s\n";
            assertEquals(expected, yamlString);

            fault = injecter.initialFault(config, "leader_zone_storage_all_net_delay_latency_032ms.yaml");
            faultPath = fault.file;
            // read yaml file
            input = new FileInputStream(faultPath);
            HashMap<String, Object> describe2 = yaml.load(input);
            writer = new StringWriter();
            yamlDumper.dump(describe2, writer);
            yamlString = writer.toString();
            input.close();

            String expected2 = "kind: PhysicalMachineChaos" +
                    "\napiVersion: chaos-mesh.org/v1alpha1" +
                    "\nmetadata:" +
                    "\n  namespace: chaos-testing" +
                    "\n  name: net-delay" +
                    "\nspec:" +
                    "\n  action: network-delay" +
                    "\n  address:" +
                    "\n  - http://133.133.135.156:31767" +
                    "\n  mode: all" +
                    "\n  network-delay:" +
                    "\n    device: em1" +
                    "\n    ip-protocol: all" +
                    "\n    latency: 32ms" +
                    "\n  duration: 120s\n";

            assertEquals(expected2, yamlString);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
