package com.github.pgsqlio.benchmarksql.chaos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import com.jcraft.jsch.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChaosClient {
	static Logger logger = LogManager.getLogger(ChaosInjecter.class);
	private static final ExecutorService executorService = Executors.newFixedThreadPool(6);
	// TODO: read from configuration
	private final String keyFile = System.getProperty("user.home") + "/.ssh/id_rsa";

	public static void main(String[] args) {
		// example parameters
		String k8scli = "wy@133.133.135.56";
		// get current path
		String file = System.getProperty("user.dir") + "/src/main/resources/faults/debug.yaml";
		// System.out.println(file);

		ChaosClient client = new ChaosClient();
		client.inject(k8scli, file, 0);
	}

	public void inject(ChaosFault fault) {
		inject(fault.k8scli, fault.file, fault.duration);
	}

	public void inject(String k8scli, String file, int duration) {
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
					break;
				}
				try {
					Thread.sleep(100);
				} catch (Exception ee) {
				}
			}
			execChannel.disconnect();

			// 异步执行 kubectl delete 命令
			executorService.submit(() -> {
				try {
					if (duration > 0) {
						Thread.sleep(duration);
					}

					// 执行 kubectl delete 命令
					execChannel.setCommand("kubectl delete -f " + remoteFilePath);
					execChannel.connect();
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

				} catch (Exception e) {
					e.printStackTrace();
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
