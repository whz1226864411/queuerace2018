package io.openmessaging;

/**
 * Created by Administrator on 2018-07-06.
 */
public class IndexResult {
    private int logIndex;
    private int readIndex;

    public IndexResult(int logIndex, int readIndex){
        this.logIndex = logIndex;
        this.readIndex = readIndex;
    }

    public int getLogIndex() {
        return logIndex;
    }

    public void setLogIndex(int logIndex) {
        this.logIndex = logIndex;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public void setReadIndex(int readIndex) {
        this.readIndex = readIndex;
    }
}
