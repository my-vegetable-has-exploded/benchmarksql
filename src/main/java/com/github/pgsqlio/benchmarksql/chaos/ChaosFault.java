package com.github.pgsqlio.benchmarksql.chaos;

public class ChaosFault {
	public String ip;
	public int port;
	public String cmd;
	// recovery time in ms
	public int duration;
	
	public ChaosFault(String ip, int port, String cmd) {
		this.ip = ip;
		this.port = port;
		this.cmd = cmd;
		this.duration = 0;
	}

	public ChaosFault(String ip, int port, String cmd, int duration) {
		this.ip = ip;
		this.port = port;
		this.cmd = cmd;
		this.duration = duration;
	}
}
