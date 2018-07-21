package com.alibaba.mwrace2018.geeks;

import java.util.List;

abstract public class IdlePageManager {
    public static final long PAGE_SIZE_4K = 4096;

    /**
     * @return page首地址列表
     */
    abstract List<Long> getPage(int pageNum);

    abstract long getOnePage();

    /**
     * 释放page，把page设置为空闲
     * @param pageList 待释放的page号列表
     */
    abstract void recyclePage(List<Long> pageList);

    /**
     * 获取页大小
     * @return 页大小
     */
    abstract int pageSize();
}
