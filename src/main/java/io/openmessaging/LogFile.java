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
import java.util.HashSet;
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

    public int appendMessage(byte[] message , int logIndex, MappedByteBuffer indexFile,int pos,long start){
        int size = 4;
        size += message.length;
        if(CommitLog.FILE_SIZE - writeIndex - 1 >= size){
            mappedByteBuffer.put((byte) (message.length >>> 24));
            mappedByteBuffer.put((byte) (message.length >>> 16));
            mappedByteBuffer.put((byte) (message.length >>> 8));
            mappedByteBuffer.put((byte) message.length);
            mappedByteBuffer.put(message);

            indexFile.position((int) (start + pos));
            indexFile.put((byte) (logIndex >>> 24));
            indexFile.put((byte) (logIndex >>> 16));
            indexFile.put((byte) (logIndex >>> 8));
            indexFile.put((byte) logIndex);
            indexFile.put((byte) (writeIndex >>> 24));
            indexFile.put((byte) (writeIndex >>> 16));
            indexFile.put((byte) (writeIndex >>> 8));
            indexFile.put((byte) writeIndex);
//            System.out.println(writeIndex);
            writeIndex += size;

            return LogFile.SUCCESS;
        } else {
            return LogFile.END_FILE;
        }
    }

    public byte[] getMessage(int msgIndex) {
        mappedByteBuffer.position(0);
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        byteBuffer.position(msgIndex);
        int length = byteBuffer.getInt();
        System.out.println("length="+length);
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        return bytes;
    }

    public void flush(){
        this.mappedByteBuffer.force();
    }
}
