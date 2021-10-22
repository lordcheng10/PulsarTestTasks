package tasks;

import org.apache.pulsar.client.api.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class NAckTask {

    public static void main(String[] args) throws ExecutionException, InterruptedException, PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();
        Producer<byte[]> producer = pulsarClient.newProducer().producerName("chenlin-producer1").topic("test9256").create();


        try {
            Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic("test9256")
                    .subscriptionName("sub9253")
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .subscriptionType(SubscriptionType.Shared)
                    .negativeAckRedeliveryDelay(0, TimeUnit.MILLISECONDS)
                    .ackTimeout(1000, TimeUnit.MILLISECONDS)
                    .subscribe();


            producer.sendAsync("1111".getBytes()).get();
            producer.sendAsync("2222".getBytes()).get();
            producer.sendAsync("3333".getBytes()).get();
            producer.sendAsync("4444".getBytes()).get();

            for (int i = 0; i < 3; i++) {
                Message msg = consumer.receive();
                consumer.negativeAcknowledge(msg);
                System.out.println(new String(msg.getData()));
            }

            System.out.println("negativeAcknowledge after.");
            for (int i = 0; i < 5; i++) {
                System.out.println("start " + i);
                Message msg = consumer.receive();
                consumer.acknowledgeAsync(msg);
                System.out.println(new String(msg.getData()));
            }

            pulsarClient.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
