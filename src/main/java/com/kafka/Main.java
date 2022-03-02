package com.kafka;

import com.kafka.consumer.KafkaConsumerAssignApp;
import com.kafka.consumer.KafkaConsumerSubscribeApp;

public class Main {
    public static void main(String[] args){
        var subscriberApp = new KafkaConsumerSubscribeApp();
        subscriberApp.ConsumeMessages();
        var assignedApp = new KafkaConsumerAssignApp();
    }
}
