package tasks;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;

public class DLQTask {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();
        String topic = "dead-letter-topic";

        final int maxRedeliveryCount = 2;

        final int sendMessages = 100;

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliveryCount).build())
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();


        Consumer<byte[]>  deadLetterConsumer = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build()
                .newConsumer(Schema.BYTES)
                .topic("dead-letter-topic-my-subscription-DLQ")
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
            totalReceived++;
        } while (totalReceived < sendMessages * (maxRedeliveryCount + 1));


        int totalInDeadLetter = 0;
        do {
            Message message = deadLetterConsumer.receive();
            if(message==null){
                System.out.println("dead letter:msg=" + new String(message.getData()));
            }else{
                System.out.println("dead letter:msg=" + new String(message.getData()));
            }
            deadLetterConsumer.acknowledge(message);
            totalInDeadLetter++;
        } while (totalInDeadLetter < sendMessages);

        deadLetterConsumer.close();
        consumer.close();

    }
}
