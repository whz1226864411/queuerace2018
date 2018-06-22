package io.openmessaging;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018-06-20.
 */
public class LogWriter {
    private AtomicInteger atomicInteger = new AtomicInteger();
    private BufferedOutputStream bufferedOutputStream;

    public LogWriter(String path){
        try {
            File file = new File(path);
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(path,true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(String message) throws IOException {
        byte[] a = new byte[4];
        Bytes.int2bytes(atomicInteger.getAndIncrement(),a,0);
        bufferedOutputStream.write(a);
        byte[] b = new byte[4];
        Bytes.int2bytes(message.length(),b,0);
        bufferedOutputStream.write(b);
        bufferedOutputStream.write(message.getBytes());
        bufferedOutputStream.flush();
    }

}
