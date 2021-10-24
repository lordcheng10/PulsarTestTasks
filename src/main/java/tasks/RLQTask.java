package tasks;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

public class RLQTask {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();
        String topic = "retry-topic";

        final int maxRedeliveryCount = 2;

        final int sendMessages = 100;

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliveryCount).build())
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();


        Consumer<byte[]>  deadLetterConsumer = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build()
                .newConsumer(Schema.BYTES)
                .topic("retry-topic-my-subscription-DLQ")
                .subscriptionName("my-subscription3")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();



        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        for (int i = 0; i < sendMessages; i++) {
            producer.send(String.format("Hello Pulsar [%d]", i).getBytes());
        }

        producer.close();

        int totalReceived = 0;
        do {
            Message<byte[]> message = consumer.receive();
            System.out.println(new String(message.getData()));
            consumer.reconsumeLater(message, 1 , TimeUnit.SECONDS);
            totalReceived++;
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));

        int totalInDeadLetter = 0;
        do {
            Message message = deadLetterConsumer.receive();
            System.out.println("dead letter:msg=" + new String(message.getData()));
            deadLetterConsumer.acknowledge(message);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        deadLetterConsumer.close();
        consumer.close();

        Consumer<byte[]> checkConsumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription4")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<byte[]> checkMessage = checkConsumer.receive(3, TimeUnit.SECONDS);
        if (checkMessage != null) {
            System.out.println("checkMessage"+ new String(checkMessage.getData()));
        }

    }
}
