package com.sdu.stream.bean;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author hanhan.zhang
 * */
public class KafkaMessage {

    public static String []USER_IDS;

    static {
        USER_IDS = new String[100];
        for (int i = 0 ; i < 100; ++i) {
            USER_IDS[i] = UUID.randomUUID().toString();
        }
    }

    public static String []ACTIONS = new String[]{"search", "click", "order"};

    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private String userId;

    private String action;

    private String timestamp;

    KafkaMessage(String userId, String action, String timestamp) {
        this.userId = userId;
        this.action = action;
        this.timestamp = timestamp;
    }

    public static KafkaMessage createKafkaMessage() {
        String userId = USER_IDS[ThreadLocalRandom.current().nextInt(USER_IDS.length)];
        String action = ACTIONS[ThreadLocalRandom.current().nextInt(ACTIONS.length)];
        String time = LocalDateTime.now().format(formatter);
        return new KafkaMessage(userId, action, time);
    }
}
