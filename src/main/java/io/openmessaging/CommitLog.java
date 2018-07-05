package io.openmessaging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-06-23.
 */
public class CommitLog {
//    private static final String ROOT_PATH = "E:/queue2018/commitlog/";
    private static final String ROOT_PATH = "/alidata1/race2018/data/";
    private List<LogFile> logFileList = new ArrayList<>();
    private Map<String, List<MessageIndex>> indexMap =  new HashMap<>();
    private final AtomicInteger nowIndex = new AtomicInteger(0);
    private volatile LogFile nowLogFile = null;
    public static final int FILE_SIZE = 1024 * 1024 * 1024;
    private Object writeLock = new Object();

    //索引
    private Map<String,Index> indexStartMap = new HashMap<>();
    private long start = 0;
    private IndexFile[] indexFileList = new IndexFile[100];
    public static final int FOUR_K = 4*1024;
    private Map<String,MappedByteBuffer> mappedByteBufferHashMap = new HashMap<>();
    //

    public CommitLog(){
        String path = ROOT_PATH + "0.log";
        createLogFile(path);
    }

    public Index getIndex(String queueName){
        Index index = indexStartMap.get(queueName);
        if (index == null){
            index = new Index();
            index.setStart(start);
            start += FOUR_K;
            indexStartMap.put(queueName,index);
        }
        return index;
    }

    public IndexFile getIndexFile(int pos) throws IOException {
        int i = pos / FOUR_K;
        IndexFile indexFile = null;
        if (indexFileList[i] != null){
            indexFile = indexFileList[i];
        }else {
            String path = ROOT_PATH + i +".index";
            File file = new File(path);
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");
            randomAccessFile.setLength(4*1024*1024*1024L);
            indexFile = new IndexFile();
            FileChannel fileChannel = randomAccessFile.getChannel();
            indexFile.setFileChannel(fileChannel);
            indexFileList[i] = indexFile;
        }
        return indexFile;
    }



    public void putMessage(String queueName, byte[] message){
        synchronized (writeLock){
            try {
                Index index = getIndex(queueName);//耗费了3s
                int pos = index.getWritePos();
                IndexFile indexFile = getIndexFile(pos);

                MappedByteBuffer mappedByteBuffer = indexFile.getMappedByteBuffer(index.getStart());
                int result = this.nowLogFile.appendMessage( message ,nowIndex.get(),mappedByteBuffer,(pos % 4096),(index.getStart() % IndexFile.MIDDLE));
                //int result = this.nowLogFile.appendMessage( message ,nowIndex.get(),null,0,0);
                if(result == LogFile.END_FILE){
                    int  i = this.nowIndex.getAndIncrement();
                    String path = ROOT_PATH + String.valueOf(i) + ".log";
                    createLogFile(path);
                    putMessage(queueName,message);
                }else {
                    index.increaseWritePos();
                    index.increaseCount();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public Collection<byte[]> getMessage(String queueName, long offset, long num){
        List<byte[]> result = new ArrayList<>();

        try {
            Index index = getIndex(queueName);
            long start = index.getStart();
            int readPos = (int) offset * 8;
            int count = index.getCount();
            IndexFile indexFile = getIndexFile(readPos);
            MappedByteBuffer mappedByteBuffer = indexFile.getMappedByteBuffer(start);
            mappedByteBuffer.position(0);
            ByteBuffer byteBuffer = mappedByteBuffer.slice();
            start %= IndexFile.MIDDLE;
            start += readPos;
            for (int i = 0; i < num; i++) {
                if (offset < count){//数量达到
                    byteBuffer.position((int) (start + i*8));
                    int logIndex = byteBuffer.getInt();//可以优化，拿出来
                    int readIndex = byteBuffer.getInt();
                    LogFile logFile = logFileList.get(logIndex);
                    System.out.println("readIndex=" + readIndex);
                    byte[] bytes = logFile.getMessage(readIndex);
                    result.add(bytes);
                }else {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }



    public void createLogFile(String path){
        File file = new File(path);
        LogFile logFile = new LogFile(file);
        this.nowLogFile = logFile;
        logFileList.add(logFile);
    }

    public LogFile getNowLogFile(){
        return this.nowLogFile;
    }


}
