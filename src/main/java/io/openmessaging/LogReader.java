package io.openmessaging;

import sun.rmi.runtime.Log;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-06-20.
 */
public class LogReader {
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    public LogReader(File file){
        try {
            randomAccessFile = new RandomAccessFile(file,"rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public List<String> read(long offset, long num) throws IOException {
        fileChannel.position(offset);
        List<String> result = new ArrayList<>();
        fileChannel.position(0);
        for (int i = 0; i < num; i++) {
            fileChannel.position(fileChannel.position() + 4);
            ByteBuffer sizeBuf = ByteBuffer.allocate(4);
            fileChannel.read(sizeBuf);
            int size = Bytes.bytes2int(sizeBuf.array());
            System.out.println(size);
            ByteBuffer content = ByteBuffer.allocate(size);
            fileChannel.read(content);
            result.add(new String(content.array()));
        }
        return result;
    }

}
