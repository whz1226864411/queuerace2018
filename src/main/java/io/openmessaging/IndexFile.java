package io.openmessaging;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Administrator on 2018-07-05.
 */
public class IndexFile {
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer = null;
    public static final long MIDDLE = 1024*1024*1024L;

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public void setFileChannel(FileChannel fileChannel) {
        try {
            this.fileChannel = fileChannel;
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,MIDDLE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MappedByteBuffer getMappedByteBuffer(){
        return mappedByteBuffer;
    }
}
