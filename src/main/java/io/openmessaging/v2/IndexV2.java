package io.openmessaging.v2;

import java.util.ArrayList;

/**
 * Created by Administrator on 2018-07-07.
 */
public class IndexV2 {
    private int start;//起始位置
    private short[] offsetList = new short[125];//每个数据文件第一个消息的offset,大小需要扩容
    private short writePos = 0;//数据文件的写指针
    private short indexPos = -1;//索引写指针,也可以代表当前日志的位置
    private short count = 0;//消息数量

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
