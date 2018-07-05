package io.openmessaging;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-07-03.
 */
public class Index {
    private long start;
    private int writePos;
    private int count;

    private static AtomicInteger atomicInteger = new AtomicInteger();

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }


    public int getWritePos() {
        return writePos;
    }

    public void setWritePos(int writePos) {
        this.writePos = writePos;
    }




    public void increaseWritePos(){
        this.writePos += 8;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void increaseCount(){
        this.count++;
    }
}
