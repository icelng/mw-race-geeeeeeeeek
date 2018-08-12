package com.alibaba.mwrace2018.geeks;

import java.util.List;

abstract public class MessageQueue {

    public void shortToBytes(short v, byte[] b, int off) {
        b[off + 1] = (byte) v;
        b[off + 0] = (byte) (v >>> 8);
    }

    public short bytesToShort(byte[] b, int off) {
        return (short) (((b[off + 1] & 0xFFL) << 0) +
                        (((long) b[off + 0] & 0xFFL) << 8));

    }


    abstract void put(byte message[], int len);
    abstract List<byte[]> get(long startIndex, long len);
    abstract void commit();
}
