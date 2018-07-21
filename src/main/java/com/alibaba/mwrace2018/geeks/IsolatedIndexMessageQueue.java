package com.alibaba.mwrace2018.geeks;

import java.util.ArrayList;
import java.util.List;

public class IsolatedIndexMessageQueue extends MessageQueue{
    private static final int DEFAULT_BLOCK_SIZE = 32;
    private static final int DEFAULT_MAX_QUEUE_LENGTH = 2048;
    private static final int INDEX_ENTRY_SIZE = 8;

    private int blockSize;
    private long blockMask;
    private long pageSize;
    private long pageMask;
    private int maxQueueLength;

    private int queueLength;
    private List<Long> indexPages;  // 索引页列表
    private long tailIdleAddress;

    private byte[] indexEntryBytes = new byte[8];

    private IdlePageManager idlePageManager;  // 空闲页管理
    private StoreIO storeIO;  // 存储IO

    public IsolatedIndexMessageQueue(IdlePageManager idlePageManager, StoreIO storeIO) {
        this(idlePageManager, storeIO, DEFAULT_BLOCK_SIZE, DEFAULT_MAX_QUEUE_LENGTH);
    }

    public IsolatedIndexMessageQueue(IdlePageManager idlePageManager, StoreIO storeIO, int blockSize, int initialQueueLen) {
        this.idlePageManager = idlePageManager;
        this.storeIO = storeIO;
        queueLength = 0;
        this.indexPages = new ArrayList<>();
        this.blockSize = blockSize;
        this.blockMask = blockSize * idlePageManager.pageSize() - 1;
        this.pageSize = idlePageManager.pageSize();
        this.pageMask = pageSize - 1;
        this.maxQueueLength = initialQueueLen;

        /*为索引项分配页*/
        int indexPagesNum = (initialQueueLen * INDEX_ENTRY_SIZE) / idlePageManager.pageSize() + 1;
        this.maxQueueLength = (int) ((indexPagesNum * pageSize)/ INDEX_ENTRY_SIZE);
        List<Long> idlePages = idlePageManager.getPage(indexPagesNum);
        for (int i = 0;i < indexPagesNum;i++) {
            indexPages.add(idlePages.get(i));
        }

        /*预分配数据项*/
        idlePages = idlePageManager.getPage(blockSize);
        tailIdleAddress = idlePages.get(0);
    }

    public int length() {
        return this.queueLength;
    }

    /**
     * 1.保证一个消息数据不能占两页
     * 2.判断是否需要开辟新快
     * 3.写入新的索引项
     * 4.写入新的消息数据
     * @param message
     */
    public void put(byte message[], int len) {

        if (queueLength >= maxQueueLength) {
            expendMaxQueueLen();
//            throw new IndexOutOfBoundsException(String.format("The queueLength is greater than maxQueueLength(%d)!", maxQueueLength));
        }

        /*保证一个数据不能占两页*/
        long adjustIdleAddress = (tailIdleAddress - 1 + message.length) & (~pageMask);
        if (adjustIdleAddress != ((tailIdleAddress - 1) & (~pageMask))) {
            tailIdleAddress = adjustIdleAddress;
        }

        /*是否需要开辟新块*/
        if (((tailIdleAddress - 1 + len) & (~blockMask)) != ((tailIdleAddress - 1) & (~blockMask))) {
            List<Long> idlePages = idlePageManager.getPage(blockSize);
            tailIdleAddress = idlePages.get(0);
        }

        /*写入新索引项*/
        indexEntryToBytes(tailIdleAddress, len, indexEntryBytes, 0);
        long newIndexEntryAddress = calIndexEntryAddress(queueLength);
        storeIO.write(newIndexEntryAddress, indexEntryBytes, 0, INDEX_ENTRY_SIZE);

        /*写入消息数据*/
        storeIO.write(tailIdleAddress, message, 0, len);
        tailIdleAddress += len;
        queueLength++;

    }

    private void expendMaxQueueLen(){
        List<Long> newIndexPage = idlePageManager.getPage(1);
        indexPages.add(newIndexPage.get(0));
        maxQueueLength += (pageSize / INDEX_ENTRY_SIZE);
    }

    /**
     * 对于每条消息数据：
     * 1.读取消息对应的索引项
     * 2.分析索引项，得到消息数据的首地址和大小
     * 3.根据首地址和大小读取消息数据
     * @param startIndex
     * @param len
     * @return
     */
    public List<byte[]> get(long startIndex, long len) {
        List<byte[]> messages = new ArrayList<>();

        if (startIndex >= queueLength) {
            return messages;
        }

        long adjustLen = queueLength - startIndex < len ? queueLength - startIndex : len;

        for (int i = 0;i < adjustLen;i++) {
            /*读索引项*/
            long indexEntryAddress = calIndexEntryAddress(startIndex + i);
            storeIO.read(indexEntryAddress, indexEntryBytes, 0, INDEX_ENTRY_SIZE);

            /*由索引项获得消息数据的地址和大小*/
            long messageAddress = bytesToMessageAddress(indexEntryBytes, 0);
            long messageSize = bytesToMessageSize(indexEntryBytes, 0);

            /*读取数据*/
            byte[] message = new byte[(int) messageSize];
            storeIO.read(messageAddress, message, 0, message.length);
            messages.add(message);
        }

        return messages;
    }

    @Override
    public void commit() {

    }

    /**
     * 计算索引项地址
     * @return
     */
    private long calIndexEntryAddress(long index) {
        int pageIndex = (int) ((INDEX_ENTRY_SIZE * (index)) / pageSize);
        long offsetInPage = (INDEX_ENTRY_SIZE * (index)) % pageSize;
        return indexPages.get(pageIndex) + offsetInPage;
    }

    /**
     * 消息首地址48 bits
     * 消息长度 16 bits
     * 大端存储
     * @param startMessageAddress
     * @param messageLength
     * @param b
     * @param off
     */
    private void indexEntryToBytes(long startMessageAddress, long messageLength, byte[] b, int off) {
        b[off + 7] = (byte) startMessageAddress;
        b[off + 6] = (byte) (startMessageAddress >>> 8);
        b[off + 5] = (byte) (startMessageAddress >>> 16);
        b[off + 4] = (byte) (startMessageAddress >>> 24);
        b[off + 3] = (byte) (startMessageAddress >>> 32);
        b[off + 2] = (byte) (startMessageAddress >>> 40);
        b[off + 1] = (byte) messageLength;
        b[off + 0] = (byte) (messageLength >>> 8);
    }

    private long bytesToMessageAddress(byte[] b, int off) {
        return ((b[off + 7] & 0xFFL) << 0) +
                ((b[off + 6] & 0xFFL) << 8) +
                ((b[off + 5] & 0xFFL) << 16) +
                ((b[off + 4] & 0xFFL) << 24) +
                ((b[off + 3] & 0xFFL) << 32) +
                (((long) b[off + 2] & 0xFFL) << 40);
    }

    private long bytesToMessageSize(byte[] b, int off) {
        return  ((b[off + 1] & 0xFFL) << 0) +
                (((long) b[off + 0] & 0xFFL) << 8);
    }

}
