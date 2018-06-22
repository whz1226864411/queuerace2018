package io.openmessaging;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2018-06-20.
 */
public class LogController {
    private static final String ROOT_PATH = "E:/queuelog";
    private static ConcurrentHashMap<String, LogWriter> map = new ConcurrentHashMap<>();

    static {
        File file = new File(ROOT_PATH);
        if(!file.exists()){
            file.mkdirs();
        }
    }


    public LogWriter getLogWriter(String topic){
        if(map.containsKey(topic)){
            LogWriter logWriter = map.get(topic);
            return logWriter;
        }else {
            String path = ROOT_PATH + "/" + topic + ".log";
            LogWriter logWriter = new LogWriter(path);
            map.put(topic,logWriter);//此处有并发问题
            return logWriter;
        }
    }

    public LogReader gerLogReader(String topic){
        String path = ROOT_PATH + "/" + topic + ".log";
        File file = new File(path);
        if(!file.exists()){
            return null;
        }else {
            return new LogReader(file);
        }
    }

}
