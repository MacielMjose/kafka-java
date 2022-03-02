package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerSubscribeApp {

    Properties props = new Properties();
    ArrayList<String> topics = new ArrayList<>();

    private KafkaConsumer consumer;

    public KafkaConsumerSubscribeApp(){
        props.put("bootstrap.servers","localhost:9092");//avoid just one bootstrap-server
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","test");
        this.consumer = new KafkaConsumer(props);
    }

    public void ConsumeMessages(){

        var topics = new ArrayList<String>();
        topics.add("my_topic");
        topics.add("my_topic_2");

        SubscribeToTopic(topics);

        try{
            while (true){
                ConsumerRecords<String,String> records = this.consumer.poll(10);
                for(ConsumerRecord<String, String> record : records){
                    //process each record
                    System.out.println(String.format("Topic: %s, Partitions: %d, Offset: %d, Key: %s, Value: %s",
                            record.topic(),record.partition(),record.offset(),record.key(),record.value()));
                }
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }finally{
            this.consumer.close();
        }
    }

    private void SubscribeToTopic(ArrayList<String> topicNames){
        this.consumer.subscribe(topicNames);
    }
}
