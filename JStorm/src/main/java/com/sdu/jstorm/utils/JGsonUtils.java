package com.sdu.jstorm.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author hanhan.zhang
 * */
public class JGsonUtils {

    private static final Gson GSON = new GsonBuilder().create();

    private static final Gson FORMAT_GSON = new GsonBuilder().setPrettyPrinting().create();

    public static <T> T fromJson(String json, Class<T> clazz) {
        return GSON.fromJson(json, clazz);
    }

    public static String formatJson(Object object) {
        return FORMAT_GSON.toJson(object);
    }
}
