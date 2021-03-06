package io.openmessaging.v2;

import io.openmessaging.CommitLog;
import io.openmessaging.LogFile;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-07-07.
 */
public class LogFileV2 {

    private RandomAccessFile randomAccessFile;
    private MappedByteBuffer readMap;
    private volatile MappedByteBuffer writeMap;
    private FileChannel fileChannel;
    private File file;
    public final static int SUCCESS = 200;
    public final static int END_FILE = 300;
    public final static int BLOCK_SIZE = 300;//1024,600

    public final static int SIXTY_FOUR_SIZE = 64*1024*1024;
    public final static short END = 0;
//    private ThreadLocal<FileChannel> threadLocal = new ThreadLocal<>();
   // private AtomicInteger atomicInteger = new AtomicInteger(0);


    public LogFileV2(File file) {
        try {
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            this.file = file;
            this.randomAccessFile = new RandomAccessFile(file,"rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.readMap = fileChannel.map(FileChannel.MapMode.READ_WRITE,0, CommitLogV2.FILE_SIZE);
            this.writeMap = fileChannel.map(FileChannel.MapMode.READ_WRITE,0, CommitLogV2.FILE_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int appendMessage(byte[] message ,IndexV2 indexV2) throws IOException {
//        if (indexV2.getWritePos() == 0){
//            atomicInteger.getAndIncrement();
//        }
        short length = (short) message.length;
        short writePos = indexV2.getWritePos();
        int start = indexV2.getStart();
        int remain = LogFileV2.BLOCK_SIZE - writePos;
        short size = (short) (2 + length);
        ByteBuffer byteBuffer = writeMap.slice();
        if( remain >= size){
            byteBuffer.position(start + writePos);//定位
            byteBuffer.put((byte) (length >>> 8));
            byteBuffer.put((byte) length);
            byteBuffer.put(message);

            //修改索引
            indexV2.increaseWritePos(size);
            return LogFileV2.SUCCESS;
        } else {
            if (remain >= 2) {
                byteBuffer.position(start + writePos);//定位
                byteBuffer.put((byte) (END >>> 8));
                byteBuffer.put((byte) END);
            }
            return LogFileV2.END_FILE;
        }
    }

    public int appendMessageV2(byte[] message ,IndexV2 indexV2) throws IOException {
//        if (indexV2.getWritePos() == 0){
//            atomicInteger.getAndIncrement();
//        }
        short length = (short) message.length;
        short writePos = indexV2.getWritePos();
        int start = indexV2.getStart();
        int remain = LogFileV2.BLOCK_SIZE - writePos;
        short size = (short) (2 + length);
        ByteBuffer byteBuffer = indexV2.getWriteBuf();
        if( remain >= size){
            byteBuffer.position(writePos);//定位
            byteBuffer.put((byte) (length >>> 8));
            byteBuffer.put((byte) length);
            byteBuffer.put(message);

            //修改索引
            indexV2.increaseWritePos(size);
            return LogFileV2.SUCCESS;
        } else {
            if (remain >= 2) {
                byteBuffer.position(writePos);//定位
                byteBuffer.put((byte) (END >>> 8));
                byteBuffer.put((byte) END);
            }
            byteBuffer.flip();
//            FileChannel fileChannel = getFileChannel();
            synchronized (fileChannel){
                fileChannel.position(start);//定位
                fileChannel.write(byteBuffer);
            }
            byteBuffer.clear();

            return LogFileV2.END_FILE;
        }
    }

//    public FileChannel getFileChannel(){
//        FileChannel fileChannel = threadLocal.get();
//        if (fileChannel == null){
//            try {
//                fileChannel = new RandomAccessFile(file,"rw").getChannel();
//                threadLocal.set(fileChannel);
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//        }
//        return fileChannel;
//    }

//    public void flush(){
//        try {
//            fileChannel.force(true);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


//    public void decrease(){
//        int i = atomicInteger.decrementAndGet();
//        if (i == 0){
//            //ReleaseUtil.releaseMap(writeMap);
//            try {
//                System.out.println("刷");
//                fileChannel.force(true);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    public MappedByteBuffer getMappedByteBuffer(){
        return readMap;
    }
}
