package com.sdu.stream.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author hanhan.zhang
 * */
public class GsonUtils {

    private static final Gson PRETTY_GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final Gson GSON = new GsonBuilder().create();

    public static String toPrettyJson(Object obj) {
        return PRETTY_GSON.toJson(obj);
    }

    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }
}
