package org.example.work_queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class Worker {

    private final static String QUEUE_NAME = "work_queues";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.basicQos(1); // cousumer는 최대 미처리 메시지를 n개까지 받을 수 있도록 함. n개 넘어가는 메시지는 받지 않음. autoAck = false일때만 의미 있음
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");

            try {
                doWork(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        boolean autoAck = false; // consumer가 mq로 ack를 직접 날려야만 mq는 queue에서 메시지를 삭제할 수 있음
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }

    private static void doWork(String task) throws InterruptedException {
        for(char ch : task.toCharArray()) {
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }
    }
}
