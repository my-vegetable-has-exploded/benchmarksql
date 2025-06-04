package com.github.pgsqlio.benchmarksql.chaos;

import com.github.pgsqlio.benchmarksql.jtpcc.SystemConfig;
import com.github.pgsqlio.benchmarksql.jtpcc.jTPCC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Iterator;

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
      // replace keys
      Set<Object> keys = map.keySet();
      for (Object key : keys) {
        if (key instanceof String) {
          String keyStr = (String) key;
          if (keyStr.startsWith("$")) {
            Object value = map.get(key);
            map.remove(key);
            map.put(getReplacement(keyStr, replacements), value);
          }
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
    // 构建一个忽略大小写的 replacements 映射
    TreeMap<String, Object> caseInsensitiveReplacements = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    caseInsensitiveReplacements.putAll(replacements);
    if (value.startsWith("$")) {
      if (value.equals("$storage.volumePath") || value.equals("$VOLUMEPATH")) {
        System.err.println("value: " + value);
      }
      Object replacement1 = caseInsensitiveReplacements.get(value);
      if (replacement1 != null) {
        return replacement1;
      }
      // map $componentNAME to componentname
      Object replacement2 = caseInsensitiveReplacements.get(value.substring(1));
      if (replacement2 != null) {
        return replacement2;
      }
    }
    return value;
  }

  public List<String> generateScope(SystemConfig config, String placeholder) throws Exception {
    // Scope placeholder need to replace by list of components,
    // scope placeholder constraints are connect by '-',
    // constraint describe the fault inject scope,
    // for example, zone describe components position of zone,
    // zone.leader means components need to be in leader zone,
    // zone.follower.1 means components need to be in follower1 zone.
    // zone.random.1 means any random zone, zone.random2 means any one random zones
    // but different from zone.random1.
    // the second last constraint is the role, there is storage and compute, default
    // the last constraint is the component list number, for example, 1 means only
    // one
    // component, 0 means all components satisfy the constraints.
    String[] constraints = placeholder.substring(1).split("-");
    ArrayList<String> components = new ArrayList<>(config.components);
    ArrayList<String> zones = null;
    // zone constraints
    String constraint0 = constraints[0];
    // zone constraints
    if (constraint0.startsWith("zone")) {
      String[] parts = constraint0.split("\\.");
      // use cached scopes
      if (config.scopesCache.containsKey(parts[0] + "." + parts[1])) {
        zones = (ArrayList<String>) config.scopesCache.get(parts[0] + "." + parts[1]);
      } else if (parts[1].equals("leader") || parts[1].equals("follower")) {
        if (config.leaderzone == null) {
          throw new Exception("leader zone not set");
        }
        if (config.zones.size() == 0) {
          throw new Exception("zones not set");
        }
        if (parts[1].equals("leader")) {
          zones = new ArrayList<String>();
          zones.add(config.leaderzone);
        } else {
          zones = new ArrayList<>();
          for (int i = 0; i < config.zones.size(); i++) {
            if (config.zones.get(i).equals(config.leaderzone)) {
              continue;
            }
            zones.add(config.zones.get(i));
          }
          // shuffle zones
          java.util.Collections.shuffle(zones, new java.util.Random());
        }
      } else if (parts[1].equals("random")) {
        if (config.leaderzone != null) {
          throw new Exception("leader zone set, skip random zone");
        }
        if (config.zones.size() == 0) {
          throw new Exception("zones not set");
        }
        zones = new ArrayList<String>(config.zones);
        java.util.Collections.shuffle(zones, new java.util.Random());
      } else {
        if (config.leaderzone != null) {
          throw new Exception("leader zone set, skip not specified zone");
        }
        throw new Exception("invalid zone constraint: " + constraint0);
      }
      // cache zones
      config.scopesCache.put(parts[0] + "." + parts[1], zones);
      String zone = null;
      if (parts.length > 2) {
        zone = (String) zones.get(Integer.parseInt(parts[2]) - 1);
      } else {
        zone = (String) zones.get(0);
      }
      components = new ArrayList<String>(config.zoneComponents.get(zone));
    }

    // role constraints
    String roleConstraint = constraints[constraints.length - 2];
    if (roleConstraint.equals("storage") || roleConstraint.equals("compute") || roleConstraint.equals("test")) {
      String rolecomponentsStr = config.getProp(config.p, "sys." + roleConstraint + ".components");
      if (rolecomponentsStr == null) {
        throw new Exception("role " + roleConstraint + " not set");
      }
      ArrayList<String> rolecomponents = new ArrayList<String>();
      for (String rolecomponent : rolecomponentsStr.split(",")) {
        rolecomponents.add(rolecomponent.strip());
      }
      // check if components in rolecomponents
      Iterator<String> iterator = components.iterator();
      while (iterator.hasNext()) {
        String component = iterator.next();
        // System.out.println("component: " + component);
        if (!rolecomponents.contains(component)) {
          // System.out.println("component: " + component + " not in rolecomponents");
          iterator.remove(); // 使用迭代器的 remove 方法
        }
      }
    } else {
      throw new Exception("invalid role constraint: " + roleConstraint);
    }

    if (components.size() == 0) {
      throw new Exception("no components satisfy the constraints");
    }

    // number constraints
    String numberConstraint = constraints[constraints.length - 1];
    if (numberConstraint.matches("\\d+")) {
      int n = Integer.parseInt(numberConstraint);
      if (n == 0) {
        return components;
      } else {
        components = new ArrayList<String>(components);
        java.util.Collections.shuffle(components, new java.util.Random());
        return components.subList(0, n);
      }
    } else {
      throw new Exception("invalid number constraint: " + numberConstraint);
    }
  }

  public HashMap<String, Object> computeReplacements(SystemConfig config, ArrayList<String> placeholders)
      throws Exception {
    HashMap<String, Object> replacements = new HashMap<String, Object>();
    for (String placeholder : placeholders) {
      // generate ips or components
      if (placeholder.contains("-")) {
        ArrayList<String> scope = (ArrayList<String>) generateScope(config, placeholder);
        ArrayList<Component> components = new ArrayList<Component>();
        ArrayList<String> chaosdList = new ArrayList<String>();
        for (String comp : scope) {
          Component component = new Component(comp);
          components.add(component);
          // convert ip to chaosd server http address
          chaosdList.add("http://" + component.host + ":31767");
        }
        // set process to components format by toString
        ArrayList<String> processList = new ArrayList<String>();
        for (Component component : components) {
          processList.add(component.toString());
        }
        replacements.put("$process", processList);
        replacements.put(placeholder, chaosdList);
      }
    }
    // extend replacements systemconfig with config.confs
    for (Map.Entry<String, String> entry : config.confs.entrySet()) {
      replacements.put("$" + entry.getKey().toUpperCase(), configParse(entry.getValue()));
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

  public static int durationParse(String duration) throws Exception {
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
    try {
      logger.info("injecting fault: {}, duration: {}", fault.file, fault.duration);
      client.inject(fault);
    } catch (Exception e) {
      logger.error("inject fault error: {}", e.getMessage());
    }
  }
}
