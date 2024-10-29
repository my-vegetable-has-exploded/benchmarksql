package com.github.pgsqlio.benchmarksql.chaos;

public class ChaosFault {
	public String k8scli;
	public String file;
	// recovery time in ms
	public int duration;

	public ChaosFault(String k8scli, String file) {
		this.k8scli = k8scli;
		this.file = file;
	}	
	
	public ChaosFault(String k8scli, String file, int duration) {
		this.k8scli = k8scli;
		this.file = file;
		this.duration = duration;
	}	
}
