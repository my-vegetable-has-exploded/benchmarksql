package com.github.pgsqlio.benchmarksql.chaos;

import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// ChaosInjecter - Inject chaos faults
public class ChaosInjecter {
	Logger logger = LogManager.getLogger(ChaosInjecter.class);
	private static ChaosInjecter instance;
	private ArrayList<ChaosFault> faults = new ArrayList<ChaosFault>();

	private ChaosInjecter() {
		// private constructor to prevent instantiation
		faults.add(new ChaosFault("133.133.135.51", 9526, "create process kill --process observer --signal 9"));
		faults.add(new ChaosFault("133.133.135.51", 9526, "create network delay --time 25 --interface ens6f1", 30000));
	}

	public static ChaosInjecter getInstance() {
		if (instance == null) {
			synchronized (ChaosInjecter.class) {
				if (instance == null) {
					instance = new ChaosInjecter();
				}
			}
		}
		return instance;
	}

	public static void main(String[] args) throws InterruptedException {
		ChaosInjecter injecter = ChaosInjecter.getInstance();
		injecter.inject(1);
		Thread.sleep(40000);
    }

	public void addFault(ChaosFault fault) {
		faults.add(fault);
	}

	public ChaosResult inject(int index) {
		ChaosFault fault = faults.get(index);
		return this.inject(fault);
	}

	public ChaosResult inject(ChaosFault fault) {
		ChaosResult result = ChaosClient.sendGetRequest(fault);
		logger.info("inject fault: " + fault.cmd + ", result: " + result);
		// create a new thread to recover the fault
		String faultId = result.getResult();
		if (fault.duration != 0 && faultId != null) {
			new Thread(() -> {
				try {
					Thread.sleep(fault.duration);
					// send recovery command to the same ip and port, but cmd is "destroy faultId"
					ChaosClient.sendGetRequest(fault.ip, fault.port, "destroy " + faultId);
					logger.info("recover fault: " + fault.cmd + ", faultId: " + faultId + ", duration: " + fault.duration);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}).start();
		}
		return result;
	}
}
