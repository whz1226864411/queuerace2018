package io.openmessaging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    public CommitLog(){
        String path = ROOT_PATH + "0.log";
        createLogFile(path);
    }


    public void putMessage(String queueName, byte[] message){
//        int lastIndex = nowIndex.get();
//        int result = this.nowLogFile.appendMessage(queueName, message);
//        if(result == LogFile.END_FILE){
//            synchronized (CommitLog.class) {
//                int index = nowIndex.get();
//                if(index == lastIndex){
//                    int  i = this.nowIndex.getAndIncrement();
//                    String path = ROOT_PATH + String.valueOf(i) + ".log";
//                    createLogFile(path);
//                }
//            }
//            putMessage(queueName,message);
//        }
        synchronized (writeLock){
            int result = this.nowLogFile.appendMessage(queueName, message,indexMap,nowIndex.get());
            if(result == LogFile.END_FILE){
                int  i = this.nowIndex.getAndIncrement();
                String path = ROOT_PATH + String.valueOf(i) + ".log";
                createLogFile(path);
                putMessage(queueName,message);
            }
        }
    }

    public Collection<byte[]> getMessage(String queueName, long offset, long num){
        List<byte[]> result = new ArrayList<>();
        List<MessageIndex> indexList = indexMap.get(queueName);
        for (int i = 0; i < num; i++) {
            int index = (int) offset + i;
            if(index < indexList.size()){
                MessageIndex messageIndex = indexList.get(index);
                int logIndex = messageIndex.getLogIndex();
                int msgIndex = messageIndex.getMessageIndex();
                LogFile logFile = logFileList.get(logIndex);
                byte[] bytes = logFile.getMessage(msgIndex);
                result.add(bytes);
            }
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
