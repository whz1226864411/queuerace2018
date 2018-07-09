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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-07-07.
 */
public class LogFileV2 {

    private RandomAccessFile randomAccessFile;
    private MappedByteBuffer readMap;
    private FileChannel fileChannel;
    private MappedByteBuffer[] writeMap = new MappedByteBuffer[1];
    private AtomicInteger[] umapSize = new AtomicInteger[1];
    public final static int SUCCESS = 200;
    public final static int END_FILE = 300;
    public final static int BLOCK_SIZE = 1024;

    public final static int SIXTY_FOUR_SIZE = 1024*1024*1024;
    public final static short END = 0;

    public LogFileV2(File file) {
        try {
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            this.randomAccessFile = new RandomAccessFile(file,"rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.readMap = fileChannel.map(FileChannel.MapMode.READ_WRITE,0, CommitLogV2.FILE_SIZE);
            for (int i = 0; i < 1; i++) {
                this.writeMap[i] = fileChannel.map(FileChannel.MapMode.READ_WRITE,i*SIXTY_FOUR_SIZE,SIXTY_FOUR_SIZE);
                umapSize[i] = new AtomicInteger(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int appendMessage(byte[] message ,IndexV2 indexV2) throws IOException {
        short length = (short) message.length;
        short writePos = indexV2.getWritePos();
        int start = indexV2.getStart();
        if (writePos == 0){
            umapSize[start/SIXTY_FOUR_SIZE].getAndIncrement();
        }
        int remain = LogFileV2.BLOCK_SIZE - writePos;
        short size = (short) (2 + length);
        ByteBuffer byteBuffer = writeMap[start/SIXTY_FOUR_SIZE].slice();
        int jidian = start % SIXTY_FOUR_SIZE;
        if( remain >= size){
            byteBuffer.position(jidian + writePos);//定位
            byteBuffer.put((byte) (length >>> 8));
            byteBuffer.put((byte) length);
            byteBuffer.put(message);

            //修改索引
            indexV2.increaseWritePos(size);
            return LogFileV2.SUCCESS;
        } else {
            if (remain >= 2) {
                byteBuffer.position(jidian + writePos);//定位
                byteBuffer.put((byte) (END >>> 8));
                byteBuffer.put((byte) END);
            }
            return LogFileV2.END_FILE;
        }
    }

    public void decrease(IndexV2 indexV2){
        int start = indexV2.getStart();
        int i = umapSize[start/SIXTY_FOUR_SIZE].decrementAndGet();
        if (i == 0){
            ReleaseUtil.releaseMap(writeMap[start/SIXTY_FOUR_SIZE]);
        }
    }

    public MappedByteBuffer getMappedByteBuffer(){
        return readMap;
    }
}
