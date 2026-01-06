package org.example.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;


public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "direct"); // 거래소 정의

            String severity = getSeverity(args);
            String message = getMessage(args);

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes(StandardCharsets.UTF_8)); // 메시지 발행
            System.out.printf("[X] Sent %s : %s\n", severity, message);
        }
    }

    private static String getMessage(String[] args) {
        if (args.length < 2) {
            return "Hello World!";
        }
        return joinStrings(args, " ", 1);
    }

    private static String joinStrings(String[] args, String delimiter, int startIndex) {
        int length = args.length;

        if (length == 0) {
            return "";
        }
        if (length <= startIndex) {
            return "";
        }

        StringBuilder words = new StringBuilder(args[startIndex]);
        for (int i = startIndex; i < length; i++) {
            words.append(delimiter)
                    .append(args[i]);
        }

        return words.toString();
    }

    private static String getSeverity(String[] args) {
        if (args.length < 1) {
            return "INFO";
        }

        return args[0];
    }

}
