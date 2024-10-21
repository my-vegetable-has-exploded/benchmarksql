package com.github.pgsqlio.benchmarksql.chaos;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ChaosClient {
    public static void main(String[] args) {
        // example parameters
        String ip = "133.133.135.51";
        int port = 9526;
        String cmd = "create process kill --process hello --signal 9";
		// String cmd = "create network delay --time 10 --interface ens6f1";

        // 调用方法发送请求并获取结果
        ChaosResult result = sendGetRequest(ip, port, cmd);

        // 输出结果
        System.out.println(result);
    }

	public static ChaosResult sendGetRequest(ChaosFault fault) {
		return sendGetRequest(fault.ip, fault.port, fault.cmd);
	}

    public static ChaosResult sendGetRequest(String ip, int port, String cmd) {
        try {
			// 对cmd参数进行URL编码
            String encodedCmd = URLEncoder.encode(cmd, StandardCharsets.UTF_8.toString());

            // 构建URL
            String url = String.format("http://%s:%d/chaosblade?cmd=%s", ip, port, encodedCmd);

            // 创建HttpClient对象
            HttpClient client = HttpClient.newHttpClient();

            // 创建HttpRequest对象
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .GET()
                    .build();

            // 发送请求并获取响应
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            // 解析响应内容
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(response.body());

            int code = jsonNode.get("code").asInt();
            boolean success = jsonNode.get("success").asBoolean();
            String result = jsonNode.has("result") ? jsonNode.get("result").asText() : null;
            String error = jsonNode.has("error") ? jsonNode.get("error").asText() : null;

            // 返回结果对象
            return new ChaosResult(code, success, result, error);
        } catch (Exception e) {
            e.printStackTrace();
            return new ChaosResult(-1, false, null, e.getMessage());
        }
    }
}
