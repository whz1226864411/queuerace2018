package io.openmessaging;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2018-07-07.
 */
public class FastMap implements Map<String,Index>{
    private MyMap[] myMaps = new MyMap[1000];


    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object queueName) {
        int i = hash((String) queueName) % 1000;
        MyMap map = myMaps[i];
        if (map == null){
            return false;
        }else {
            return map.containsKey(queueName);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public Index get(Object queueName) {
        int i = hash((String) queueName) % 1000;
        MyMap map = myMaps[i];
        if (map == null){
            return null;
        }else {
            return map.get((String) queueName);
        }
    }

    @Override
    public Index put(String queueName, Index index){
        int i = hash(queueName) % 1000;
        MyMap map = myMaps[i];
        if (map == null){
            map = new MyMap();
            myMaps[i] = map;
        }
        return map.put(queueName,index);
    }

    @Override
    public Index remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends Index> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<String> keySet() {
        return null;
    }

    @Override
    public Collection<Index> values() {
        return null;
    }

    @Override
    public Set<Entry<String, Index>> entrySet() {
        return null;
    }

    public int hash(String queueName){
        int hash = queueName.hashCode();
        if (hash < 0){
            hash = -hash;
        }
        return hash;
    }


}
