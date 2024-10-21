package com.github.pgsqlio.benchmarksql.chaos;

public class ChaosResult {
    private int code;
    private boolean success;
    private String result;
    private String error;

    // 构造函数
    public ChaosResult(int code, boolean success, String result, String error) {
        this.code = code;
        this.success = success;
        this.result = result;
        this.error = error;
    }

    // Getter 方法
    public int getCode() {
        return code;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getResult() {
        return result;
    }

    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        return "ChaosResult{" +
                "code=" + code +
                ", success=" + success +
                ", result='" + result + '\'' +
                ", error='" + error + '\'' +
                '}';
    }	
}
