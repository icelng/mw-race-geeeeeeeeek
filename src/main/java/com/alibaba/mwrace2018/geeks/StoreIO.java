package com.alibaba.mwrace2018.geeks;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StoreIO {
    public  int regionMask;

    private long regionSize;
    private int regionBitsLen;
    private long regionsNum;
    private List<MappedByteBuffer> originRegions;
    private ThreadLocal<List<ByteBuffer>> threadLocalRegions = new ThreadLocal<>();

    public StoreIO(String filePath, long fileSize, int regionSize) {

        regionSize--;
        this.regionBitsLen = 0;
        while(regionSize > 0) {
            this.regionBitsLen++;
            regionSize = regionSize >>> 1;
        }

        this.regionSize = 1 << this.regionBitsLen;
        this.regionMask = (int) (this.regionSize - 1);
        this.regionsNum = fileSize >>> this.regionBitsLen;
        this.originRegions = new ArrayList<>();

        try {
            /*每次测试都删除原来的文件*/
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }

            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw");
            randomAccessFile.setLength(fileSize);  // 设置文件大小
            FileChannel fileChannel = randomAccessFile.getChannel();
            for (int i = 0;i < regionsNum;i++) {
                originRegions.add(fileChannel.map(FileChannel.MapMode.READ_WRITE, i * this.regionSize, this.regionSize));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        startFlushService();

    }

    /**
     * 读数据,该方法很有可能会引起IO操作
     * readSize 不能大于一个region中，addr开始，剩余的长度
     * @param addr
     * @param buf
     * @param readSize
     */
    public void read(long addr, byte buf[], int offset, int readSize) {
        ByteBuffer region = getRegion(addr);
        region.position((int) (addr & regionMask));
        region.get(buf, offset, readSize);
    }

    /**
     * 零拷贝读，实际上是从MappedByteBuffer上slice出一段ByteBuffer出来，不会引起IO操作
     * readSize 不能大于一个region中addr开始的剩余的长度
     * @param addr
     * @param readSize
     * @return
     */
    public ByteBuffer read(long addr, int readSize) {

        ByteBuffer region = getRegion(addr);
        region.position((int) (addr & regionMask));
        ByteBuffer byteBufferNew = region.slice();
        byteBufferNew.limit(readSize);

        return byteBufferNew;
    }

    /**
     * 写入数据
     * writeSize 不能大于一个region中，addr开始，剩余的长度
     * @param addr
     * @param buf
     * @param writeSize
     */
    public void write(long addr, byte buf[], int offset, int writeSize) {

        ByteBuffer region = getRegion(addr);
        region.position((int) (addr & regionMask));
        region.put(buf, offset, writeSize);

    }

    public void write(long addr, ByteBuffer writeBuffer) {

        ByteBuffer region = getRegion(addr);
        region.position((int) (addr & regionMask));
        region.put(writeBuffer);

    }

    /**
     * 使用线程栈保存regions，减少ByteBuffer的slice
     * @param addr
     * @return
     */
    public ByteBuffer getRegion(long addr) {
        List<ByteBuffer> regions = threadLocalRegions.get();
        if (regions == null || regions.size() != originRegions.size()) {
            regions = new ArrayList<>();
            for (MappedByteBuffer originRegion : originRegions) {
                regions.add(originRegion.slice());
            }
            threadLocalRegions.set(regions);
        }

        int regionNo = (int) (addr >>> regionBitsLen);
        if (regionNo > regionsNum || addr < 0) {
            throw new IndexOutOfBoundsException();
        }
        ByteBuffer region = regions.get(regionNo);
        region.clear();
        return region;
    }

    public void flush() {
        for (MappedByteBuffer region : originRegions) {
            region.force();
        }
    }

    private void startFlushService() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::flush, 100, 3000, TimeUnit.MILLISECONDS);
    }

    public static void main(String arg[]) {
        Thread[] threads = new Thread[16];
        long start = System.currentTimeMillis();
        AtomicLong writeSize = new AtomicLong(0);
        try {
            byte[] bytes = "12345678".getBytes("UTF-8");
            int bytesSize = bytes.length;
            String filePath = "./test.log";
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }

            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw");
            FileChannel fileChannel = randomAccessFile.getChannel();
            long regionSize = 256 * 1024 * 1024;
            randomAccessFile.setLength(16 * regionSize);  // 设置文件大小

            for (int i = 0;i < 16;i++) {
                MappedByteBuffer region = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, i * regionSize, regionSize);
                long positionStart = i * regionSize;
                threads[i] = new Thread(() -> {
                    int writeTimes = (int) (regionSize >> 3);
                    for (int j = 0;j < writeTimes;j++) {
                        try {
                            ByteBuffer writeByteBuffer = ByteBuffer.wrap(bytes);
                            fileChannel.write(writeByteBuffer, positionStart + j * bytesSize);
                            //region.put(bytes);
                            if (writeSize.addAndGet(bytesSize) > 4096 * 1024 * 8) {
                                writeSize.set(0);
                                System.out.println("Force!!!");
                                fileChannel.force(false);
                                //region.force();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
                threads[i].start();
            }

            for (int i = 0;i < 4;i++) {
                threads[i].join();
            }

            System.out.println(System.currentTimeMillis() - start);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
