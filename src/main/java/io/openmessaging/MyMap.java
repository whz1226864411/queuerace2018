package io.openmessaging;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018-07-07.
 */
public class MyMap {
    private Map<String,Index> indexStartMap = new HashMap<>();

    public Index get(String queueName) {
        return indexStartMap.get(queueName);
    }

    public Index put(String queueName, Index index){
        return indexStartMap.put(queueName,index);
    }

    public boolean containsKey(Object queueName) {
        return indexStartMap.containsKey(queueName);
    }
}
