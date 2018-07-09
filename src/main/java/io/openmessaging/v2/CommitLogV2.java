package io.openmessaging.v2;

import io.openmessaging.Index;
import io.openmessaging.LogFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-07-07.
 */
public class CommitLogV2 {
    private static final String ROOT_PATH = "/alidata1/race2018/data/";
    private List<LogFileV2> logFileList = new ArrayList<>();
    private volatile int nowIndex = -1;
    public static final int FILE_SIZE = 1024 * 1024 * 1024;
    private Object writeLock = new Object();
    private Object createFileLock = new Object();

    //索引
    private Map<String, IndexV2> indexV2Map = new ConcurrentHashMap<>();
//    private Map<Integer, IndexV2> indexV2Map = new HashMap<>();
    private int start = 0;

    public CommitLogV2(){
        createLogFile((short) 0);
    }

    public void createLogFile(short indexPos){//创建数据文件
        synchronized (createFileLock){
            if (logFileList.size() - 1 < indexPos){
                this.nowIndex++;
                String path = ROOT_PATH + this.nowIndex + ".log";
                File file = new File(path);
                LogFileV2 logFile = new LogFileV2(file);
                logFileList.add(logFile);
                logFileList.size();
            }
        }
    }

    public IndexV2 getIndexV2(String queueName) {//获取索引
        IndexV2 indexV2 = indexV2Map.get(queueName);
        if (indexV2 == null){
            synchronized (indexV2Map){
                indexV2 = indexV2Map.get(queueName);
                if (indexV2 == null){
                    indexV2 = new IndexV2();
                    indexV2.setStart(start);
                    start += LogFileV2.BLOCK_SIZE;
                    indexV2Map.put(queueName, indexV2);
                }
            }
        }
        return indexV2;
    }

    public void putMessage(String queueName, byte[] message, IndexV2 indexV2) {
            try {
                if (indexV2 == null){
                    indexV2 = getIndexV2(queueName);
                }
                synchronized (indexV2){
                    short indexPos = indexV2.getIndexPos();
                    LogFileV2 logFileV2 = logFileList.get(indexPos);
                    int result = logFileV2.appendMessage(message,indexV2);
                    if (result == LogFileV2.END_FILE){
                        logFileV2.decrease(indexV2);
                        indexV2.insert();
                        indexPos = indexV2.getIndexPos();
                        //System.out.println(indexPos);
                        if (logFileList.size() - 1 < indexPos){
                            createLogFile(indexPos);
                        }
                        putMessage(queueName,message,indexV2);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

    }


    public Collection<byte[]> getMessage(String queueName, long offset, long num,
                                         List<byte[]> result,IndexV2 indexV2, int logIndex){
        if (result == null){
            result = new ArrayList<>();
            indexV2 = getIndexV2(queueName);
            logIndex = indexV2.search((short) offset);
        }
        int start = indexV2.getStart();
        short initOffset = indexV2.getInitOffset(logIndex);
        int readPos = start;
        short readSize = 0;
        LogFileV2 logFileV2 = logFileList.get(logIndex);
        MappedByteBuffer mappedByteBuffer = logFileV2.getMappedByteBuffer();
        //mappedByteBuffer.position(0);
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(readPos);
        short length = 0;
        for (short i = 0; i < (offset - initOffset); i++) {
            length = byteBuffer.getShort();
            int j = 2+ length;
            readPos += j;
            readSize += j;
            byteBuffer.position(readPos);
        }
        for (int i = 0; i < num; i++) {
            if (offset + i  < indexV2.getCount()){
                if ( LogFileV2.BLOCK_SIZE - readSize >= 2 && (length = byteBuffer.getShort()) != 0){
                    byte[] bytes = new byte[length];
                    byteBuffer.get(bytes);
                    result.add(bytes);
                    readSize += 2 + length;
                }else {
                    getMessage(queueName,offset + i, (num - i),result,indexV2,logIndex + 1);
                    break;
                }
            }else {
                break;
            }
        }
        return result;
    }

}
