package com.alibaba.mwrace2018.geeks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 一次性分配空闲页，不回收
 * 使用顺序分配方式
 */
public class SingleUseIdlePageManager extends IdlePageManager{
    private long totalPageNum;
    private int pageBitsLen;
    private int pageSize;

    private AtomicLong idlePageStartIndex = new AtomicLong(0);  // 空闲起始

    public SingleUseIdlePageManager(long totalSize, long pageSize) {

        /**
         * pageSize 应该为2次幂倍
         * */
        if (pageSize <= 0 || pageSize > totalSize) {
            pageSize = IdlePageManager.PAGE_SIZE_4K;
        }
        pageBitsLen = 0;
        pageSize--;
        while(pageSize > 0) {
            pageBitsLen++;
            pageSize = pageSize >>> 1;
        }

        /**
         * totalSize/pageSize 向下取整
         * */
        totalPageNum = totalSize >>> pageBitsLen;

        this.pageSize = 1 << pageBitsLen;
        System.out.println("totalSize:" + totalSize + " pageBitsLen:" + pageBitsLen + " pageSize:" + this.pageSize + " totalPageNum:" + totalPageNum);
//        getPage(1);
    }

    @Override
    public List<Long> getPage(int pageNum) {

        long afterIndex = idlePageStartIndex.addAndGet(pageNum);
        if (afterIndex > totalPageNum) {
            throw new IndexOutOfBoundsException(String.format("There is not enough idle pages, totalPageNum:%d", totalPageNum));
        }

        List<Long> pageList = new ArrayList<>();
        afterIndex -= pageNum;

        for (int i = 0;i < pageNum;i++) {
            pageList.add(afterIndex++ << pageBitsLen);
        }

        return pageList;
    }

    @Override
    long getOnePage() {

        long pageIndex = idlePageStartIndex.getAndAdd(1);
        if (pageIndex > totalPageNum) {
            throw new IndexOutOfBoundsException(String.format("There is not enough idle pages, totalPageNum:%d", totalPageNum));
        }

        return pageIndex << pageBitsLen;
    }

    @Override
    public void recyclePage(List<Long> pageList) {

    }

    @Override
    int pageSize() {
        return this.pageSize;
    }
}
