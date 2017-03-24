package com.sdu.jstorm.utils;

import java.util.Collection;

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

}
