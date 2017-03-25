package com.sdu.jstorm.utils;

import java.util.Collection;
import java.util.Map;

/**
 * Collection Util
 *
 * @author hanhan.zhang
 * */
public class JCollectionUtil {

    public static final <E> boolean isEmpty(Collection<E> collection) {
        if (collection == null) {
            return true;
        }
        return collection.isEmpty();
    }

    public static final <E> boolean isNotEmpty(Collection<E> collection) {
        return !isEmpty(collection);
    }

    public static final <K, V> boolean isEmpty(Map<K, V> map) {
        if (map == null) {
            return true;
        }
        return map.isEmpty();
    }

    public static final <K, V> boolean isNotEmpty(Map<K, V> map) {
        return !isEmpty(map);
    }
}
