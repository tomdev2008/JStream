package com.sdu.jstorm.utils;

import java.util.Map;

/**
 * Map Utils
 *
 * @author hanhan.zhang
 * */
public class MapUtil {

    public static final <K, V> boolean isEmpty(Map<K, V> map) {
        if (map == null) {
            return false;
        }
        return map.isEmpty();
    }

    public static final <K, V> boolean isNotEmpty(Map<K, V> map) {
        return !isEmpty(map);
    }

}
