package com.alibaba.mwrace2018.geeks;

import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 常驻页缓存池，优先使用堆外内存，如果不存在交换区，则操作系统发生页框回收的时候，该内存区域是不会被回收的
 */
public class ResidentPageCachePool {
    private int poolSize;
    private int pageSize;
    private Deque<ByteBuffer> byteBuffers;

    public ResidentPageCachePool(int poolSize, int pageSize) {
        this.poolSize = poolSize;
        this.pageSize = pageSize;
        byteBuffers = new ConcurrentLinkedDeque<>();


        long maxDirectMemorySize = (long) (2.4 * 1024 * 1024 * 1024);
        long directMemorySize = 0;
        long heapMemorySize = 0;

        System.out.println("The max size of heap memory is: " + (Runtime.getRuntime().maxMemory() >> 20) + "M");

        for (int i = 0;i < poolSize;i++) {
            byteBuffers.addFirst(ByteBuffer.allocate(pageSize));
            heapMemorySize += pageSize;
//            if (((long) i) * pageSize < maxDirectMemorySize) {
//                /*优先使用堆外内存*/
//                byteBuffers.addFirst(ByteBuffer.allocateDirect(pageSize));
//                directMemorySize += pageSize;
//            } else {
//                byteBuffers.addFirst(ByteBuffer.allocate(pageSize));
//                heapMemorySize += pageSize;
//            }
        }

        System.out.println(String.format("%dM DirectByteBuffer have be allocated!", directMemorySize >> 20));
        System.out.println(String.format("%dM HeapByteBuffer have be allocated!", heapMemorySize >> 20));

    }

    public ByteBuffer borrowPage() {
        return byteBuffers.pollFirst();
    }

    public void returnPage(ByteBuffer page) {
        page.clear();
        byteBuffers.addFirst(page);
    }

    public void free(ByteBuffer page) throws Exception {
        Field cleanField = page.getClass().getDeclaredField("cleaner");
        cleanField.setAccessible(true);
        Cleaner cleaner = (Cleaner) cleanField.get(page);
        cleaner.clean();
    }

}
