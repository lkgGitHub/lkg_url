package com.lkg;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class SCA_Producer extends Thread{

    public static void main(String[] args) throws InterruptedException, IOException {
        String topic = "url";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("retries", 1);//重试次数
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

//        String filePath = "D:\\01Work\\04微智URL分析\\SCA疑似URL\\data\\LTE_240_YDLNG00137_S1U103_20190728002004_0000.txt";
        String filePath = "D:\\sca\\url.txt";
        File file = new File(filePath);
        BufferedReader buf = new BufferedReader(new FileReader(file));
        String line;
        while((line=buf.readLine())!=null){
            Thread.sleep(20); //dev
//            Thread.sleep(20); //pro
            System.out.println(line);
            producer.send(new ProducerRecord<>(topic, line), (metadata, exception) -> {
                if(exception == null){
                    System.out.println("success ->" + metadata.offset());
                }else {
                    exception.printStackTrace();
                }
            });
        }

        buf.close();
        producer.close();
    }

}
