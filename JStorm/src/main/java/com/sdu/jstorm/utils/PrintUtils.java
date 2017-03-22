package com.sdu.jstorm.utils;

import org.apache.logging.log4j.util.Strings;

import java.util.Collection;

/**
 * Print Utils
 *
 * @author hanhan.zhang
 * */
public class PrintUtils {

    public static final <T> String collectionToString(Collection<T> collection, String start, String end, String separator) {
        StringBuffer sb = new StringBuffer();
        if (Strings.isNotEmpty(start)) {
            sb.append(start);
        }
        boolean first = true;
        for (T t : collection) {
            if (first) {
                sb.append(t);
                first = false;
            } else {
                sb.append(separator).append(t);
            }
        }
        if (Strings.isNotEmpty(end)) {
            sb.append(end);
        }
        return sb.toString();
    }

}
