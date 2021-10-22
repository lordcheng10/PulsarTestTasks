package tasks;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.util.RelativeTimeUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SeekTask {


    public static void main(String[] args) throws ExecutionException, InterruptedException, PulsarClientException {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        Producer<byte[]> producer = client.newProducer().producerName("chenlin-producer1").topic("test1").create();
        List<MessageId> messageIds = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();


        timestamps.add(System.currentTimeMillis());
        messageIds.add(producer.sendAsync("1111".getBytes()).get());
        Thread.sleep(1000);

        timestamps.add(System.currentTimeMillis());
        messageIds.add(producer.sendAsync("2222".getBytes()).get());
        Thread.sleep(1000);

        timestamps.add(System.currentTimeMillis());
        messageIds.add(producer.sendAsync("3333".getBytes()).get());
        Thread.sleep(1000);

        timestamps.add(System.currentTimeMillis());
        messageIds.add(producer.sendAsync("4444".getBytes()).get());

        Consumer consumer1 = client.newConsumer()
                .topic("test1")
                .subscriptionName("sb1")
                .subscribe();
        consumer1.seek(messageIds.get(2));

        Message msg1 = consumer1.receive(1000, TimeUnit.MILLISECONDS);
        System.out.println("Message received: " + (msg1 == null ? "null" : new String(msg1.getData())));
        consumer1.acknowledge(msg1);
        if ("4444".equals(new String(msg1.getData()))) {
            System.out.println("seekByMessageID success!");
        }

        consumer1.seek(timestamps.get(1));
        Message msg2 = consumer1.receive();
        System.out.println("Message received: " + (msg2 == null ? "null" : new String(msg2.getData())));
        if ("2222".equals(new String(msg2.getData()))) {
            System.out.println("seekByTime success!");
        }
        client.close();
    }
}
