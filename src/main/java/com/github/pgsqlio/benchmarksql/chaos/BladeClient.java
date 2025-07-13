package com.github.pgsqlio.benchmarksql.chaos;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import org.json.JSONObject;

public class BladeClient {

    public static void main(String[] args) {
        BladeClient client = new BladeClient();
        String host = "133.133.135.156";
        String resultId = client.executeCmd("create cpu load --cpu-percent 50", host);
        if (resultId != null) {
            System.out.println("Command executed successfully, result ID: " + resultId);
            boolean destroyed = client.destory(resultId, host);
            if (destroyed) {
                System.out.println("Successfully destroyed the command with ID: " + resultId);
            } else {
                System.out.println("Failed to destroy the command with ID: " + resultId);
            }
        } else {
            System.out.println("Failed to execute command.");
        }
    }

    // execute command on remote host using http request
    public String executeCmd(String cmd, String host) {
        // # 触发 CPU 负载 50% 场景
        // curl
        // "http://xxx.xxx.xxx.xxx:9526/chaosblade?cmd=create%20cpu%20load%20--cpu-percent%2050"
        // {"code":200,"success":true,"result":"e08a64a9af02c393"}
        // return result id
        // execute curl "http://" + host + ":9526/chaosblade?cmd=" + cmd;
        try {
            String url = "http://"+host+":9526/chaosblade";
            String params = "cmd="+cmd;

            // create http request
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .POST(BodyPublishers.ofString(params))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .build();

            // 3. send request
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

            // parse response
            JSONObject jsonResponse = new JSONObject(response.body());
            int code = jsonResponse.getInt("code");
            boolean success = jsonResponse.getBoolean("success");
            if (success == false) {
                System.out.println("Error: " + jsonResponse.getString("error"));
                return null; // return null in case of error
            }
            String result = jsonResponse.getString("result");

            // System.out.println("Code: " + code);
            // System.out.println("Success: " + success);
            // System.out.println("Result: " + result); // e2b454c4c0ff517d
            return result; // return the result id
        } catch (Exception e) {
            e.printStackTrace();
            return null; // return null in case of an error
        }
    }

    public boolean destory(String id, String host) {
        // # 销毁实验场景
        // curl "http://xxx.xxx.xxx.xxx:8080/chaosblade?cmd=destroy%20e08a64a9af02c393"
        try {
            String url = "http://"+host+":9526/chaosblade";
            String params = "cmd=destroy "+id;

            // create http request
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .POST(BodyPublishers.ofString(params))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .build();

            // 3. send request
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

            // parse response
            JSONObject jsonResponse = new JSONObject(response.body());
            int code = jsonResponse.getInt("code");
            boolean success = jsonResponse.getBoolean("success");
            if (success == false) {
                System.out.println("Error: " + jsonResponse.getString("error"));
                return false; // return null in case of error
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false; // return null in case of an error
        }
    }
}
