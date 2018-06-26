package io.openmessaging;

import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-06-25.
 */
public class LogFile {
    private RandomAccessFile randomAccessFile;
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel fileChannel;
    private int writeIndex = 0;
    private int unFlushSize = 0;
    public final static int SUCCESS = 200;
    public final static int END_FILE = 300;

    public LogFile(File file) {
        try {
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            this.randomAccessFile = new RandomAccessFile(file,"rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,CommitLog.FILE_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int appendMessage(String queueName, byte[] message,Map<String, List<MessageIndex>> indexMap,int logIndex){
        int size = 4;
//        size += queueName.length();
        size += message.length;
        if(CommitLog.FILE_SIZE - writeIndex - 1 >= size){
//            byte[] queueNameLength = new byte[4];
//            Bytes.int2bytes(queueName.length(),queueNameLength,0);
//            byte[] qName = queueName.getBytes();
            byte[] messageLength = new byte[4];
            Bytes.int2bytes(message.length ,messageLength,0);
//            mappedByteBuffer.put(queueNameLength);
//            mappedByteBuffer.put(qName);
            mappedByteBuffer.put(messageLength);
            mappedByteBuffer.put(message);
            List<MessageIndex> indexList = null;
            if(indexMap.containsKey(queueName)){
                indexList = indexMap.get(queueName);
            }else {
                indexList = new ArrayList<>();
                indexMap.put(queueName,indexList);
            }
            MessageIndex messageIndex = new MessageIndex();
            messageIndex.setLogIndex(logIndex);
            messageIndex.setMessageIndex(writeIndex);
            indexList.add(messageIndex);
            writeIndex += size;
            //System.out.println("wrindex="+writeIndex);
            unFlushSize += size;
//            if(unFlushSize > 4*1024){
//              //  System.out.println("刷出");
//                mappedByteBuffer.force();
//                unFlushSize = 0;
//            }
            return LogFile.SUCCESS;
        } else {
            mappedByteBuffer.force();
            return LogFile.END_FILE;
        }
    }

    public byte[] getMessage(int msgIndex) {
        mappedByteBuffer.position(0);
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(msgIndex);
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        return bytes;
    }

    public void flush(){
        this.mappedByteBuffer.force();
    }
}
