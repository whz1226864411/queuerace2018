package io.openmessaging;

import io.openmessaging.v2.CommitLogV2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

//    private CommitLog commitLog = new CommitLog();
//
//    public void put(String queueName, byte[] message) {
//        commitLog.putMessage(queueName, message);
//    }
//
//    public Collection<byte[]> get(String queueName, long offset, long num) {
//        return commitLog.getMessage(queueName, offset, num);
//    }
    private CommitLogV2 commitLogV2 = new CommitLogV2();

    public void put(String queueName, byte[] message) {
        commitLogV2.putMessage(queueName, message,null);
    }

    public Collection<byte[]> get(String queueName, long offset, long num) {
        return commitLogV2.getMessage(queueName,offset,num,null,null,0);
    }


}
