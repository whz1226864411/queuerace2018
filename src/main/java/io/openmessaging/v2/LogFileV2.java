package io.openmessaging.v2;

import io.openmessaging.CommitLog;
import io.openmessaging.LogFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * Created by Administrator on 2018-07-07.
 */
public class LogFileV2 {

    private RandomAccessFile randomAccessFile;
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel fileChannel;
    public final static int SUCCESS = 200;
    public final static int END_FILE = 300;
    public final static int BLOCK_SIZE = 1024;

    public LogFileV2(File file) {
        try {
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            this.randomAccessFile = new RandomAccessFile(file,"rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,0, CommitLogV2.FILE_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int appendMessage(byte[] message ,IndexV2 indexV2) throws IOException {
        short length = (short) message.length;
        short writePos = indexV2.getWritePos();
        int start = indexV2.getStart();
        int remain = LogFileV2.BLOCK_SIZE - writePos - 1;
        short size = (short) (2 + length);
        if( remain >= size){
            mappedByteBuffer.position(start + writePos);//定位
            mappedByteBuffer.put((byte) (length >>> 8));
            mappedByteBuffer.put((byte) length);
            mappedByteBuffer.put(message);

            //修改索引
            indexV2.increaseWritePos(size);
            return LogFileV2.SUCCESS;
        } else {
            if (remain >= 2) {
                mappedByteBuffer.position(start + writePos);//定位
                mappedByteBuffer.put((byte) (-1 >>> 8));
                mappedByteBuffer.put((byte) -1);
            }
            return LogFileV2.END_FILE;
        }
    }

    public MappedByteBuffer getMappedByteBuffer(){
        return mappedByteBuffer;
    }
}
