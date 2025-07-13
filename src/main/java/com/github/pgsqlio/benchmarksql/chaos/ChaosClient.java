package com.github.pgsqlio.benchmarksql.chaos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import com.github.pgsqlio.benchmarksql.jtpcc.jTPCC;
import com.jcraft.jsch.*;

public class ChaosClient {
	static Logger logger = LogManager.getLogger(ChaosInjecter.class);
	// TODO: read from configuration
	private final String keyFile = System.getProperty("user.home") + "/.ssh/id_rsa";

	public static void main(String[] args) {
		// example parameters
		String k8scli = "wy@133.133.135.56";
		// get current path
		String file = System.getProperty("user.dir") + "/src/main/resources/faults/debug.yaml";
		// System.out.println(file);

		ChaosClient client = new ChaosClient();
		client.inject_by_server(k8scli, file, 0);
	}

	public void inject(ChaosFault fault) throws Exception {
		// resolve fault file
		String filePath = fault.file;
		InputStream faultFile = new FileInputStream(filePath);
		Yaml yaml = new Yaml();
		HashMap<String, Object> faultDesc = yaml.load(faultFile);
		// get fault type by spec.action
		HashMap<String, Object> spec = (HashMap<String, Object>) faultDesc.get("spec");
		String action = (String) spec.get("action");

		// if action is "process" or "disk-fill", inject by chaosblade agent
		// or inject by kubectl apply
		if (action.equals("process") || action.equals("disk-fill")) {
			// currently chaosd can't handle process and disk-fill properly
			// so we use chaosblade agent to inject these faults
			inject_by_agent(faultDesc, fault.file);
		} else {
			inject_by_server(fault.k8scli, fault.file, fault.duration);
		}
	}

	// inject fault by calling chaosblade agent at corresponding node
	public void inject_by_agent(HashMap<String, Object> faultDesc, String faultFile) throws Exception {
		Connection conn = null;
		ArrayList<String> fault_ids = new ArrayList<>();

		try {
			Class.forName("org.sqlite.JDBC");
			// connect to sqlite database
			conn = DriverManager.getConnection("jdbc:sqlite:chaosbench.db");
			// create a inject_history table if not exists
			// create table if not exists inject_history (id TEXT, host TEXT)
			String createTableSQL = "CREATE TABLE IF NOT EXISTS inject_history (id TEXT, host TEXT);";
			conn.createStatement().execute(createTableSQL);
			// get the duration from faultDesc
			HashMap<String, Object> spec = (HashMap<String, Object>) faultDesc.get("spec");
			final int duration = ChaosInjecter.durationParse((String) spec.get("duration"));

			// get action from faultDesc
			String action = (String) spec.get("action");
            final long startTime = System.currentTimeMillis();
			switch (action) {
				case "process":
					fault_ids.addAll(inject_by_agent_process(spec, conn));
					break;
				case "disk-fill":
					fault_ids.addAll(inject_by_agent_disk_fill(spec, conn));
					break;
				default:
					break;
			}
			// if fault_ids is not empty, then we need to destroy the faults after duration
			new Thread() {
				public void run() {
					try {
						if (duration > 0) {
							Thread.sleep(duration);
						} else {
							// wait for injection to take effect
							Thread.sleep(10000);
						}
						destory_faults(fault_ids, faultFile,  startTime, duration);
					} catch (Exception e) {
						logger.error("Error in destory_faults: " + e.getMessage());
					}
				}
			}.start();
		} catch (Exception e) {
			logger.error("Error in inject_by_agent: " + e.getMessage());
		} finally {
			// close the connection
			try {
				if (conn != null) {
					conn.setAutoCommit(true);
					conn.close();
				}
			} catch (SQLException ex) {
				System.out.println(ex.getMessage());
			}
		}
	}

	public void destory_faults(ArrayList<String> fault_ids, String faultFile, long startTime, int duration) {
		Connection conn = null;
		try {
			Class.forName("org.sqlite.JDBC");
			// connect to sqlite database
			conn = DriverManager.getConnection("jdbc:sqlite:chaosbench.db");
			BladeClient bladeClient = new BladeClient();
			for (String fault_id : fault_ids) {
				// get host from history table
				String host = conn.createStatement()
						.executeQuery("SELECT host FROM inject_history WHERE id = '" + fault_id + "'")
						.getString("host");
				boolean res = bladeClient.destory(fault_id, host);
				if (res) {
					logger.info("Destroy fault " + fault_id + " on " + host);
					// delete the record from history table
					conn.createStatement().execute("DELETE FROM inject_history WHERE id = '" + fault_id + "';");
					// conn.commit();
                    jTPCC.csv_fault_write(faultFile + "," + startTime + "," + System.currentTimeMillis() + "," + duration + "\n");
					jTPCC.copyFaultFile(faultFile);
				} else {
					logger.error("Failed to destroy fault " + fault_id + " on " + host);
				}
			}
		} catch (Exception e) {
			logger.error("Error in destory_faults: " + e.getMessage());
		} finally {
			// close the connection
			try {
				if (conn != null) {
					conn.setAutoCommit(true);
					conn.close();
				}
			} catch (SQLException ex) {
				System.out.println(ex.getMessage());
			}
		}
	}

	public ArrayList<String> inject_by_agent_process(HashMap<String, Object> spec, Connection conn) {
		try {
			// get the process from spec
			ArrayList<Component> components = new ArrayList<>();
			HashMap<String, Object> process = (HashMap<String, Object>) spec.get("process");
			ArrayList<String> processList = (ArrayList<String>) process.get("process");
			for (String processName : processList) {
				Component component = new Component(processName);
				components.add(component);
			}

			ArrayList<String> results = new ArrayList<>();
			BladeClient bladeClient = new BladeClient();
			for (Component component : components) {
				String res = bladeClient.executeCmd("create process kill --process " + component.process + " --signal 9",
						component.host);
				if (res != null) {
					results.add(res);
					conn.createStatement()
							.execute("INSERT INTO inject_history (id, host) VALUES ('" + res + "', '" + component.host
									+ "');");
					// conn.commit();
				} else {
					logger.error(
							"Failed to execute command on " + component.host + " for process " + component.process);
				}
			}
			return results;
		} catch (Exception e) {
			logger.error("Error in inject_by_agent_process: " + e.getMessage());
			return null;
		}
	}

    public static String extractIP(String address) {
        // extract the IP address from httpserver address
        // for example , extract  http://133.133.135.156:31767 from http://133.133.135.156:31767
        String ip = address.replaceAll("http://|https://|:[0-9]+", "");
        return ip;
    }

	public ArrayList<String> inject_by_agent_disk_fill(HashMap<String, Object> spec, Connection conn) {
		try {
			// get the hosts and path from spec
			HashMap<String, Object> disk = (HashMap<String, Object>) spec.get("disk-fill");
			ArrayList<String> hostList = (ArrayList<String>) spec.get("address");
			String volumePath = (String) disk.get("path");

			ArrayList<String> results = new ArrayList<>();
			BladeClient bladeClient = new BladeClient();
			for (String host : hostList) {
				String res = bladeClient.executeCmd("create disk fill --path " + volumePath + " --reserve 0",
						extractIP(host));
				if (res != null) {
					results.add(res);
					conn.createStatement()
							.execute("INSERT INTO inject_history (id, host) VALUES ('" + res + "', '" + extractIP(host) + "');");
					// conn.commit();
				} else {
					logger.error(
							"Failed to execute command on " + host + " for disk fill operation " + volumePath);
				}
			}
			return results;
		} catch (Exception e) {
			logger.error("Error in inject_by_agent_disk_fill: " + e.getMessage());
			return null;
		}
	}

	// inject fault by applying the yaml file at k8scli
	public void inject_by_server(String k8scli, String file, int duration) {
		String[] parts = k8scli.split("@");
		final String username = parts[0];
		final String host = parts[1];

		// remote file path is under /tmp, and extract file name from file path
		String remoteFilePath = "/tmp" + file.substring(file.lastIndexOf("/"));

		try {
			JSch jsch = new JSch();
			jsch.addIdentity(keyFile);
			Session session = jsch.getSession(username, host, 22);

			// session.setPassword("your_password");

			session.setConfig("StrictHostKeyChecking", "no");

			session.connect();

			// 上传文件
			ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
			sftpChannel.connect();
			sftpChannel.put(file, remoteFilePath);
			sftpChannel.disconnect();

			// 执行 kubectl apply 命令
			final ChannelExec execChannel = (ChannelExec) session.openChannel("exec");
			execChannel.setCommand("kubectl apply -f " + remoteFilePath);
			execChannel.connect();
			final long startTime = System.currentTimeMillis();
			InputStream in = execChannel.getInputStream();
			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					logger.info(new String(tmp, 0, i));
				}
				if (execChannel.isClosed()) {
					if (in.available() > 0)
						continue;
					logger.info("Exit status: " + execChannel.getExitStatus());
					// log the output if the command failed
					if (execChannel.getExitStatus() != 0) {
						InputStream err = execChannel.getErrStream();
						byte[] errTmp = new byte[1024];
						while (err.available() > 0) {
							int i = err.read(errTmp, 0, 1024);
							if (i < 0)
								break;
							logger.error(new String(errTmp, 0, i));
						}
					}
					break;
				}
				try {
					Thread.sleep(100);
				} catch (Exception ee) {
				}
			}
			execChannel.disconnect();

			// 异步执行 kubectl delete 命令
			new Thread(() -> {
				try {
					if (duration > 0) {
						Thread.sleep(duration);
					} else {
						// wait for injection to take effect
						Thread.sleep(10000);
					}

					// 执行 kubectl delete 命令
					execChannel.setCommand("kubectl delete -f " + remoteFilePath);
					execChannel.connect();
					long endTime = System.currentTimeMillis();
					InputStream deleteIn = execChannel.getInputStream();
					byte[] deleteTmp = new byte[1024];
					while (true) {
						while (deleteIn.available() > 0) {
							int i = deleteIn.read(deleteTmp, 0, 1024);
							if (i < 0)
								break;
							logger.info(new String(deleteTmp, 0, i));
						}
						if (execChannel.isClosed()) {
							if (deleteIn.available() > 0)
								continue;
							logger.info("Exit status: " + execChannel.getExitStatus());
							// log the output if the command failed
							if (execChannel.getExitStatus() != 0) {
								InputStream err = execChannel.getErrStream();
								byte[] errTmp = new byte[1024];
								while (err.available() > 0) {
									int i = err.read(errTmp, 0, 1024);
									if (i < 0)
										break;
									logger.error(new String(errTmp, 0, i));
								}
							}
							break;
						}
						try {
							Thread.sleep(100);
						} catch (Exception ee) {
						}
					}
					execChannel.disconnect();

					// 断开连接
					session.disconnect();
					jTPCC.csv_fault_write(file + "," + startTime + "," + endTime + "," + duration + "\n");
					jTPCC.copyFaultFile(file);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}).start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
