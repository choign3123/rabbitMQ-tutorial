package org.example.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private final String REQUEST_QUEUE_NAME = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] args) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i=0; i<32; i++) {
                String iStr = Integer.toString(i);
                System.out.println("[x] Requesting fib(" + iStr + ")");
                String response = fibonacciRpc.call(iStr);
                System.out.println("[.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException, ExecutionException {
        final String corrId = UUID.randomUUID().toString();

        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties requestProperty = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", REQUEST_QUEUE_NAME, requestProperty, message.getBytes(StandardCharsets.UTF_8));

        final CompletableFuture<String> response = new CompletableFuture<>();
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String receiveCorrId = delivery.getProperties().getCorrelationId();
            if (!receiveCorrId.equals(corrId)) {
                return;
            }

            response.complete(new String(delivery.getBody(), StandardCharsets.UTF_8));
        };

        String ctag = channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> {});
        String result = response.get();
        channel.basicCancel(ctag); // 호출하지 않을시 consumer가 계속 살아있기 대문.
        return result;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
