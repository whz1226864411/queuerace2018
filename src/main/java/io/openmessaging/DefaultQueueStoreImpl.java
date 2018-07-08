package io.openmessaging;

import io.openmessaging.v2.CommitLogV2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    private AtomicInteger atomicInteger = new AtomicInteger();
    private AtomicInteger atomicInteger1 = new AtomicInteger();
    private AtomicInteger atomicInteger2 = new AtomicInteger();
    private AtomicInteger atomicInteger3 = new AtomicInteger();

    public void put(String queueName, byte[] message) {
//        if (atomicInteger.get() < 300){
//            System.out.println("queueName="+queueName+";message="+new String(message));
//            atomicInteger.getAndIncrement();
//        }
//        if (message.length > 60 && atomicInteger1.get() < 400){
//            System.out.println("message="+message.length);
//            atomicInteger1.getAndIncrement();
//        }
        commitLogV2.putMessage(queueName, message,null);
    }

    public Collection<byte[]> get(String queueName, long offset, long num) {
//        if (atomicInteger2.get() < 300){
//            System.out.println("queueName="+queueName+";offset="+offset+";num="+num);
//            atomicInteger2.getAndIncrement();
//        }

        Collection<byte[]> result = commitLogV2.getMessage(queueName,offset,num,null,null,0);
        List<byte[]> resul = (List<byte[]>) result;
        if(atomicInteger.get() > 1000000 - 20){
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("queueName="+queueName+";offset="+offset+";num="+num+";size="+resul.size());
            for (int i = 0; i < result.size(); i++) {
                stringBuilder.append("msg"+i+"="+new String(resul.get(i)));
            }
            System.out.println(stringBuilder.toString());
        }
        atomicInteger.getAndIncrement();

//        if (result.size() != num){
//            System.out.println("num=" + num + "queName=" + queueName + ";size=" + result.size() + ";off="+offset);
//        }
        return result;
    }


}
