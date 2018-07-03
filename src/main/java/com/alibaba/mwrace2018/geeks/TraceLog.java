package com.alibaba.mwrace2018.geeks;

/**
 * @author 奥陌
 */
class TraceLog {

    /**
     * 入口类型.
     */
    private final static String RPC_TYPE_ENTRY = "0";
    /**
     * 客户端类型.
     */
    private final static String RPC_TYPE_CLIENT = "1";
    /**
     * 服务端类型.
     */
    private final static String RPC_TYPE_SERVER = "2";

    /**
     * 原始数据行.
     */
    private String line;
    /**
     * Trace ID.
     */
    private String traceId;
    /**
     * RPC ID.
     */
    private String rpcId;
    /**
     * RPC 类型.
     */
    private int rpcType;
    /**
     * 用户数据.
     */
    private String userData;
    /**
     * 响应时间.
     */
    private int rt;
    /**
     * 返回值.
     */
    private String resultCode;

    TraceLog(String line) {
        this.line = line;
        String[] traceLog = line.split("\\|");

        // 日志行有三种类型：
        // 入口类型：TraceId | Timestamp(毫秒) | RpcId | 耗时(ms) | 错误码("00"-正确 "01"-错误) | 服务名 | 用户数据(key1=value1&key2=value2)
        // 		例：c0a8050115304435465056955d9b13|1530443546505|0|17|00|traceName17|@a=1&
        // 客户端型：TraceId | Timestamp(毫秒) | RpcType(固定为1) | RpcId | 服务名 | 方法名 | 对端IP | 耗时(格式[序列化耗时,总耗时]) | 错误码("00"-正确 "01"-错误) | 预留 | 预留 | 用户数据(key1=value1&key2=value2)
        //		例：c0a8050115304435433133591d9b13|1530443543320|1|0.1.1.1|service.25.2.3|methodName2523|10.0.0.253|[0, 23]|00|0|0|@a=1
        // 服务端型：TraceId | Timestamp(毫秒) | RpcType(固定为2) | RpcId | 服务名 | 方法名 | 错误码("00"-正确 "01"-错误) | 对端IP | 耗时 | 预留 | 用户数据(key1=value1&key2=value2)
        //		例：c0a8050115304435433133591d9b13|1530443543320|2|0.1.1|service.25.1.2|methodName2512|00|10.0.0.251|85|0|@a=1&s=8b3dd3fc&@ps=2898038f&
        String rpcType = traceLog[2];
        if (RPC_TYPE_CLIENT.equals(rpcType)) {
            this.rpcType = 1;
            this.traceId = traceLog[0];
            this.rpcId = traceLog[3];
            this.rt = Integer.parseInt(traceLog[7].substring(
                traceLog[7].indexOf(',') + 1, traceLog[7].indexOf(']')).trim());
            this.resultCode = traceLog[8];
            this.userData = traceLog[traceLog.length - 1];
        } else if (RPC_TYPE_SERVER.equals(rpcType)) {
            this.rpcType = 2;
            this.traceId = traceLog[0];
            this.rpcId = traceLog[3];
            this.rt = Integer.parseInt(traceLog[8]);
            this.resultCode = traceLog[6];
            this.userData = traceLog[traceLog.length - 1];
        } else if (RPC_TYPE_ENTRY.equals(rpcType)) {
            this.rpcType = 0;
            this.traceId = traceLog[0];
            // 固定为 0
            this.rpcId = "0";
            this.rt = Integer.parseInt(traceLog[3]);
            this.resultCode = traceLog[4];
            this.userData = traceLog[traceLog.length - 1];
        }
    }

    String getTraceId() {
        return this.traceId;
    }

    String getRpcId() {
        return rpcId;
    }

    int getRpcType() {
        return rpcType;
    }

    String getUserData() {
        return userData;
    }

    int getRt() {
        return rt;
    }

    String getResultCode() {
        return resultCode;
    }

    String getLine() {
        return line;
    }

}
