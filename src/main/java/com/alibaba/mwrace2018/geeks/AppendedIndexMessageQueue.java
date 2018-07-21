package com.alibaba.mwrace2018.geeks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class AppendedIndexMessageQueue extends MessageQueue{
    private static Logger logger = LoggerFactory.getLogger(AppendedIndexMessageQueue.class);

    private static final int MESSAGE_HEAD_SIZE = 2;
    private static final int INITIAL_PAGE_TABLE_LEN = 32;
    private static final int EXPEND_PAGE_TABLE_LEN = 8;

    private long[] pageTable;
    private int pageTableLen;
    private int lastPageIndex;
    private StoreIO storeIO;
    private IdlePageManager idlePageManager;
    private ResidentPageCachePool residentPageCachePool;
    private byte[] head = new byte[MESSAGE_HEAD_SIZE];

    private long pageSize;
    private long pageMask;
    private int queueLength;

    private int wrotePosition;
    private ByteBuffer residentPageCache;
    private boolean isWritePageCache = false;
    private long residentPageCacheMappedAddr;

    public AppendedIndexMessageQueue(
            IdlePageManager idlePageManager,
            StoreIO storeIO,
            ResidentPageCachePool residentPageCachePool) {

        this.storeIO = storeIO;
        this.idlePageManager = idlePageManager;
        this.residentPageCachePool = residentPageCachePool;
        this.pageSize = idlePageManager.pageSize();
        this.pageMask = pageSize - 1;
        this.queueLength = 0;

        pageTable = new long[INITIAL_PAGE_TABLE_LEN * 2];
        pageTableLen = INITIAL_PAGE_TABLE_LEN;
        lastPageIndex = 0;
        wrotePosition = 0;
        pageTable[lastPageIndex * 2] = wrotePosition;
        pageTable[lastPageIndex * 2 + 1] = queueLength;

        residentPageCacheMappedAddr = -1;

    }

    private void expendPageTable() {
        long[] newPageTable = new long[(pageTableLen + EXPEND_PAGE_TABLE_LEN) * 2];
        for (int i = 0;i < pageTableLen * 2;i++) {
            newPageTable[i] = pageTable[i];
        }
        pageTable = newPageTable;
        pageTableLen += EXPEND_PAGE_TABLE_LEN;
    }

    /**
     * 非线程安全
     * @param message
     */
    @Override
    public void put(byte[] message) {

        if (residentPageCache == null) {
            residentPageCache = residentPageCachePool.borrowPage();
            if (residentPageCache == null) {
                logger.error("Failed to borrow resident page cache!");
                throw new OutOfMemoryError("Failed to borrow resident page cache!");
            }
        }

        if (!isWritePageCache) {
            /*如果缓存用来读，则切换写模式*/
            residentPageCache.clear();
            isWritePageCache = true;
        }

        /*如果数据写满一页*/
        if (((wrotePosition + message.length + MESSAGE_HEAD_SIZE) & (~pageMask)) != (wrotePosition & (~pageMask))) {
            /*需要在新页上写*/
            /*先把常驻页内容写入pageCache*/
            if (wrotePosition == 0) {
                logger.error(String.format("Error! wrote position: %d, message size: %d", wrotePosition, message.length));
            }
            commit();
            lastPageIndex++;
            if (lastPageIndex >= pageTableLen) {
                expendPageTable();
            }
            wrotePosition = 0;
            residentPageCache.clear();
            isWritePageCache = true;
        }


        /*写入数据*/
        shortToBytes((short) message.length, head, 0);
        residentPageCache.put(head);
        residentPageCache.put(message);
        wrotePosition += (MESSAGE_HEAD_SIZE + message.length);
        pageTable[lastPageIndex * 2 + 1] = ++queueLength;

    }

    @Override
    public List<byte[]> get(long startIndex, long len) {
        ByteBuffer readPage = null;
        long adjustLen = startIndex + len > queueLength? queueLength - startIndex : len;
        int readNum;
        List<byte[]> messages = new LinkedList<>();

        if (startIndex >= queueLength) {
            return messages;
        }

        commit();

        int curPageIndex = findPageEntryIndex(startIndex);
        long messagePageAddress = pageTable[curPageIndex * 2];

        for (int i = 0;i < adjustLen;i += readNum) {

            messagePageAddress = pageTable[curPageIndex * 2];
            readNum = (int) Math.min(pageTable[curPageIndex * 2 + 1] - (startIndex + i), adjustLen - i);
            int startIndexInPage = curPageIndex == 0 ? (int) startIndex : (int) ((startIndex + i) - pageTable[(curPageIndex - 1) * 2 + 1]);

            int startPosition = 0;
            /*判断是否为常驻页缓存*/
            if (residentPageCacheMappedAddr == messagePageAddress) {
//            if (false) {
                readPage = residentPageCache;
                readPage.position(0);
                readPage.limit(readPage.capacity());
                startPosition = locateMessageInPage(readPage, startIndexInPage);
                readPage.position(startPosition);
            } else {
//                readPage = storeIO.read(messagePageAddress, (int) pageSize);
                readPage = storeIO.getRegion(messagePageAddress);
                int pageOffset = (int) (messagePageAddress & storeIO.regionMask);
                readPage.position(pageOffset);
                readPage.limit((int) (pageOffset + pageSize));
                startPosition = locateMessageInPage(readPage, startIndexInPage);
                readPage.position(pageOffset + startPosition);
            }

            for (int j = 0;j < readNum;j++) {
                readPage.get(head, 0, 2);
                short messageSize = bytesToShort(head, 0);
                byte[] message = new byte[messageSize];
                readPage.get(message);
                messages.add(message);
            }

            curPageIndex++;
        }

        ///*把最后一页数据保存到常驻缓存页*/
        if (residentPageCacheMappedAddr != messagePageAddress) {
            commit();  // 保证常驻页不是“脏页”
            if (residentPageCache == null) {
                residentPageCache = residentPageCachePool.borrowPage();
            }
            if (residentPageCache != null) {
                residentPageCacheMappedAddr = messagePageAddress;
//                readPage = storeIO.read(messagePageAddress, (int) pageSize);
                ByteBuffer region = storeIO.getRegion(messagePageAddress);
                int regionOffset = (int) (messagePageAddress & storeIO.regionMask);
                region.position(regionOffset);
                region.limit((int) (regionOffset + pageSize));
                residentPageCache.clear();
                residentPageCache.put(region);
                residentPageCache.position(0);
            }
        }

//        System.out.println("length:" + messages.size());

        return messages;
    }

    private int locateMessageInPage(ByteBuffer page, int messageNo) {
        int offset = 0;
        int savedPosition = page.position();

        for (int i = 0;i < messageNo;i++) {
            page.get(head, 0, 2);
            offset += (bytesToShort(head, 0) + MESSAGE_HEAD_SIZE);
            page.position(savedPosition + offset);
        }
        page.position(savedPosition);

        return offset;

    }

    private int findPageEntryIndex(long messageIndex) {
        int top = lastPageIndex;
        int bottom = 0;
        int mid;

        for (mid = (top + bottom) / 2;bottom < top;mid = (top + bottom) / 2) {
            if (pageTable[mid * 2 + 1] < messageIndex) {
                bottom = mid + 1;
            } else {
                top = mid;
            }
        }

        return mid;
    }

    /**
     * 提交常驻内存页内容到pageCache
     */
    @Override
    public void commit() {
        if (residentPageCache == null) {
            /*如果没有使用常驻内存页，或者常驻内存页不是作为写缓存，则不用提交*/
            return;
        }

        logger.info("commit!!!!");

        if (isWritePageCache) {
            isWritePageCache = false;
            long newPageAddress = idlePageManager.getOnePage();
            pageTable[lastPageIndex * 2] = newPageAddress;
            residentPageCache.flip();
            storeIO.write(newPageAddress, residentPageCache);
        }

//        residentPageCachePool.returnPage(residentPageCache);
//        residentPageCache = null;
    }

}
