package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.MessageFormat;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        var props = new Properties();
            props.put("bootstrap.servers","localhost:9092");//avoid just one bootstrap-server
            props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

            var kafkaProducer = new KafkaProducer(props);

            try {
                for (int i = 0; i < 150; i++){
                    //var myRecord = new ProducerRecord("my_topic",Integer.toString(i),"My Message:"+Integer.toString(i));
                    var myRecord = new ProducerRecord("replicated_topic",Integer.toString(i),"My Message:"+Integer.toString(i));
                    System.out.println("Produced message:" + myRecord.value());
                    kafkaProducer.send(myRecord);//best practice try catch
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
            finally {
                kafkaProducer.close();
            }
    }

    public static void producesMessage(String topicName, String message) throws Exception {
        if(topicName.isEmpty() || topicName == null){
            throw new Exception("invalid value for parameter topicName");
        }
        if(message.isEmpty() || message == null){
            throw new Exception("invalid value for parameter message");
        }

        var myMessage = new ProducerRecord(topicName,message);
    }
}
