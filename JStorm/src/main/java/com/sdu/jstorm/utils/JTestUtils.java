package com.sdu.jstorm.utils;

import com.google.common.collect.Lists;
import com.sdu.jstorm.test.JInputData;

import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author hanhan.zhang
 * */
public class JTestUtils {

    private static Random random = new Random();

    public static String []datas = new String[] {"the cow jumped over the moon",
                                                 "the man went to the store and bought some candy",
                                                 "four score and seven years ago",
                                                 "how many apples can you eat"};

    public static List<JInputData<String>> getData(int size) {
        List<JInputData<String>> data = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; ++i) {
            String key = UUID.randomUUID().toString();
            String value = datas[random.nextInt(datas.length)];
            data.add(new JInputData<>(key, value));
        }
        return data;
    }

}
