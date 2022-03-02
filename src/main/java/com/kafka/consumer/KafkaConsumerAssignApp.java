package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerAssignApp {

    Properties props = new Properties();
    ArrayList<String> topics = new ArrayList<>();

    private KafkaConsumer consumer;

    public KafkaConsumerAssignApp(){
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

        AssignTopics(topics);

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

    private void AssignTopics(ArrayList<String> topicNames){
        ArrayList<TopicPartition> partitions = new ArrayList<>();
        TopicPartition myTopicPart0 = new TopicPartition("my_topic", 0);
        TopicPartition myTopicPart1 = new TopicPartition("my_topic_2", 1);

        partitions.add(myTopicPart0);
        partitions.add(myTopicPart1);

        this.consumer.assign(partitions);
    }
}
