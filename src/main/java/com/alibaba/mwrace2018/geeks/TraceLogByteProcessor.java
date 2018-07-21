package com.alibaba.mwrace2018.geeks;

import com.google.common.io.ByteProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// 日志行有三种类型：
// 入口类型：TraceId | Timestamp(毫秒) | RpcId | 耗时(ms) | 错误码("00"-正确 "01"-错误) | 服务名 | 用户数据(key1=value1&key2=value2)
// 		例：c0a8050115304435465056955d9b13|1530443546505|0|17|00|traceName17|@a=1&
// 客户端型：TraceId | Timestamp(毫秒) | RpcType(固定为1) | RpcId | 服务名 | 方法名 | 对端IP | 耗时(格式[序列化耗时,总耗时]) | 错误码("00"-正确 "01"-错误) | 预留 | 预留 | 用户数据(key1=value1&key2=value2)
//		例：c0a8050115304435433133591d9b13|1530443543320|1|0.1.1.1|service.25.2.3|methodName2523|10.0.0.253|[0, 23]|00|0|0|@a=1
// 服务端型：TraceId | Timestamp(毫秒) | RpcType(固定为2) | RpcId | 服务名 | 方法名 | 错误码("00"-正确 "01"-错误) | 对端IP | 耗时 | 预留 | 用户数据(key1=value1&key2=value2)
//		例：c0a8050115304435433133591d9b13|1530443543320|2|0.1.1|service.25.1.2|methodName2512|00|10.0.0.251|85|0|@a=1&s=8b3dd3fc&@ps=2898038f&

public class TraceLogByteProcessor implements ByteProcessor<Result> {
    private static Logger logger = LoggerFactory.getLogger(TraceLogByteProcessor.class);
    private static final int TIMEOUT_THRESHOLD = 200;
//    private static final byte[] RESULT_CODE_ERROR = "01";

    /**
     * 入口类型.
     */
    private final static int RPC_TYPE_ENTRY = '0';
    /**
     * 客户端类型.
     */
    private final static int RPC_TYPE_CLIENT = '1';
    /**
     * 服务端类型.
     */
    private final static int RPC_TYPE_SERVER = '2';


    private Result result = new Result();
    private IdlePageManager idlePageManager;
    private StoreIO storeIO;
    private ResidentPageCachePool residentPageCachePool;

    private byte[] logBytesLine = new byte[256];
    private int byteIndex = 0;
    private int fieldStartIndex = 0;

    private int fieldIndex = 0;

    private byte rpcType = 0;
    private int rt = 0;
    private String userData;
    private byte[] resultCode = new byte[2];

    public TraceLogByteProcessor(IdlePageManager idlePageManager, StoreIO storeIO, ResidentPageCachePool residentPageCachePool) {
        super();
        this.idlePageManager = idlePageManager;
        this.storeIO = storeIO;
        this.residentPageCachePool = residentPageCachePool;
    }

    @Override
    public boolean processBytes(byte[] buf, int off, int len) throws IOException {
        String strTemp;
        for (int i = 0;i < len;i++) {
            byte c = buf[off + i];

            if (c == '\r') {
                continue;
            }

            logBytesLine[byteIndex++] = c;

            if (c == '|') {
                switch (fieldIndex++) {
                    case 2:
                        /*类型*/
                        rpcType = logBytesLine[fieldStartIndex];
                        break;
                    case 3:
                        /*入口的耗时*/
                        if (rpcType == RPC_TYPE_ENTRY) {
                            strTemp = new String(logBytesLine, fieldStartIndex, byteIndex - fieldStartIndex - 1);
                            this.rt = Integer.parseInt(strTemp);
//                            logger.info("type:0, rt:{}", this.rt);

                        }
                        break;
                    case 4:
                        if (rpcType == RPC_TYPE_ENTRY) {
                            /*执行结果*/
                            resultCode[0] = logBytesLine[fieldStartIndex];
                            resultCode[1] = logBytesLine[fieldStartIndex + 1];
                        }
                        break;
                    case 6:
                        /*0(入口类型)的用户数据*/
                        if (rpcType == RPC_TYPE_ENTRY) {
                            strTemp = new String(logBytesLine, fieldStartIndex, byteIndex - fieldStartIndex - 1);
                            this.userData = strTemp;
                        } else if (rpcType == RPC_TYPE_SERVER) {
                            /*执行结果*/
                            resultCode[0] = logBytesLine[fieldStartIndex];
                            resultCode[1] = logBytesLine[fieldStartIndex + 1];
                        }
                        break;
                    case 7:
                        /*client 的耗时*/
                        if (rpcType == RPC_TYPE_CLIENT) {
                            strTemp = new String(logBytesLine, fieldStartIndex, byteIndex - fieldStartIndex - 1);
                            this.rt = Integer.parseInt(strTemp.substring(
                                    strTemp.indexOf(',') + 1, strTemp.indexOf(']')).trim());
//                            logger.info("type:1, rt:{}", this.rt);
                        }
                        break;
                    case 8:
                        if (rpcType == RPC_TYPE_SERVER) {
                            /*server 耗时*/
                            strTemp = new String(logBytesLine, fieldStartIndex, byteIndex - fieldStartIndex - 1);
                            this.rt = Integer.parseInt(strTemp);
//                            logger.info("type:2, rt:{}", this.rt);
                        } else if (rpcType == RPC_TYPE_CLIENT) {
                            /*执行结果*/
                            resultCode[0] = logBytesLine[fieldStartIndex];
                            resultCode[1] = logBytesLine[fieldStartIndex + 1];
                        }
                        break;

                }
                fieldStartIndex = byteIndex;
            }else if (c == '\n') {
                /*一行解析完毕*/
                String seqNum = new String(logBytesLine, 21, 4);

                if (!this.result.getSeqNumQueueMap().containsKey(seqNum)) {
                    /*如果不存在顺序号对应的队列*/
                    this.result.getSeqNumQueueMap().put(seqNum, new AppendedIndexMessageQueue(idlePageManager, storeIO,residentPageCachePool));
//                    this.result.getSeqNumQueueMap().put(seqNum, new IsolatedIndexMessageQueue(idlePageManager, storeIO));
                }

                MessageQueue seqNumMessageQueue = this.result.getSeqNumQueueMap().get(seqNum);
                seqNumMessageQueue.put(logBytesLine, byteIndex);

                /*选择traceId*/
                if (this.select()) {
                    String traceId = new String(logBytesLine, 0, 30);
                    this.result.getTargetTraceIds().add(traceId);
                }

                fieldIndex = 0;
                fieldStartIndex = 0;
                userData = null;
                byteIndex = 0;
            }
        }

        return true;
    }

    @Override
    public Result getResult() {
        return this.result;
    }

    private boolean select() {
        if (this.rt > TIMEOUT_THRESHOLD) {
            return true;
        }
        if (resultCode[0] == '0' && resultCode[1] == '1') {
            return true;
        }

        return userData != null && userData.contains("@force=1");
    }
}
