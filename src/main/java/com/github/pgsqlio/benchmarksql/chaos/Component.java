package com.github.pgsqlio.benchmarksql.chaos;

public class Component {
    public String ip;
    public String process;
    // public String zone;
    
    public Component(String component) throws Exception {
        String[] parts = component.split(":");
        if (parts.length == 2) {
            this.ip = parts[0];
            this.process = parts[1];
        } else {
            throw new Exception("Component format error: " + component+ ", should be ip:process like  133.133.135.56:tikv");
        }
    }

    public String toString() {
        return ip + ":" + process;
    }
    
}
