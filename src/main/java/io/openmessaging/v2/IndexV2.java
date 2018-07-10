package io.openmessaging.v2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Administrator on 2018-07-07.
 */
public class IndexV2 {
    public static short INIT_SIZE = 220;
    private int start;//起始位置
    private short[] offsetList = new short[INIT_SIZE];//每个数据文件第一个消息的offset,大小需要扩容
    private short writePos = 0;//数据文件的写指针
    private short indexPos = -1;//索引写指针,也可以代表当前日志的位置
    private short count = 0;//消息数量
    private short listSize = INIT_SIZE;

    public IndexV2(){
        insert();
    }

    public int search(short offset) {//二分查找找位置
        int l = 0, h = indexPos;
        while (l <= h) {
            int m = l + (h - l) / 2;
            if (offsetList[m] <= offset) {
                l = m + 1;
            } else {
                h = m - 1;
            }
        }
        return h;
    }

    public short getInitOffset(int index){
        return offsetList[index];
    }

    public void insert() {
        if (listSize < (indexPos + 2)){
            //扩容
            listSize += 5;
            offsetList = Arrays.copyOf(offsetList,listSize);
        }
        offsetList[++indexPos] = count;
        writePos = 0;
    }

    public void increaseCount(){
        count++;
    }

    public void increaseWritePos(short size){
        writePos += size;
        count++;
    }


    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public short getIndexPos() {
        return indexPos;
    }

    public void setIndexPos(byte indexPos) {
        this.indexPos = indexPos;
    }

    public short getWritePos() {
        return writePos;
    }

    public void setWritePos(short writePos) {
        this.writePos = writePos;
    }

    public short getCount() {
        return count;
    }

    public void setCount(short count) {
        this.count = count;
    }
}
