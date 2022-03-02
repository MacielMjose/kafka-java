package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    private  KafkaProducer kafkaProducer;

    public KafkaProducerApp(){
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");//avoid just one bootstrap-server
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer(props);
    }

    public void ProduceMessages(){
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
}
