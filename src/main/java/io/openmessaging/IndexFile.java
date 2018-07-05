package io.openmessaging;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Administrator on 2018-07-05.
 */
public class IndexFile {
    private FileChannel fileChannel;
    private MappedByteBuffer[] mappedByteBuffers = new MappedByteBuffer[2];
    public static final long MIDDLE = 2*1024*1024L;

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public void setFileChannel(FileChannel fileChannel) {
        try {
            this.fileChannel = fileChannel;
            mappedByteBuffers[0] = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,MIDDLE);
            mappedByteBuffers[1] = fileChannel.map(FileChannel.MapMode.READ_WRITE,MIDDLE,MIDDLE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MappedByteBuffer getMappedByteBuffer(long start){
            if (start >= MIDDLE){
                return mappedByteBuffers[1];
            }else {
                return mappedByteBuffers[0];
            }
    }
}
