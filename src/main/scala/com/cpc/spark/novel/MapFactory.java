package com.cpc.spark.novel;

import java.util.HashMap;
import java.util.Map;

public class MapFactory<K, V> {

    private Map<K, V> mMap = new HashMap<>();

    public MapFactory() {

    }


    public MapFactory putReverseMap(Map<? extends V, ? extends K> origMap) {

        if (origMap != null) {
            for (Map.Entry<? extends V, ? extends K> entry : origMap.entrySet()) {
                V value = (V) entry.getKey();
                K key = (K) entry.getValue();
                mMap.put(key, value);
            }
        }
        return this;
    }

    public MapFactory(Map<? extends K, ? extends V> map) {
        append(map);
    }

    public MapFactory(Map.Entry<? extends K, ? extends V>... entries) {
        append(entries);
    }

    public MapFactory append(Map<? extends K, ? extends V> map) {
        mMap.putAll(map);
        return this;
    }

    public MapFactory append(Map.Entry<? extends K, ? extends V>... entries) {
        for (Map.Entry entry : entries) {
            K key = (K) entry.getKey();
            V val = (V) entry.getValue();
            mMap.put(key, val);
        }
        return this;
    }

    public MapFactory append(K key, V value) {
        mMap.put(key, value);
        return this;
    }

    public Map<K, V> getMap() {
        return mMap;
    }
}
