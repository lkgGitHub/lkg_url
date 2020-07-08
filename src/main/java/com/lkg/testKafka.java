package com.lkg;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class testKafka {

    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Usage: Predict <kafkaServers> <kafkaTopic> <kafkaGroupId> <kafkaReset> <kafkaUrlIndex>");
            System.exit(1);
        }
        int duration = Integer.parseInt(args[0]); //10
        String kafkaServers = args[1]; //"localhost:9092"
        String kafkaTopic = args[2]; // "url"
        String kafkaGroupId = args[3]; //scaGroup
        String kafkaReset = args[4]; //
        int kafkaUrlIndex = Integer.parseInt(args[5]); //.toInt // 76


        SparkConf sparkConf = new SparkConf().setAppName("test kafka");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(duration * 1000));

        // 读取kafka数据
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaGroupId);
        kafkaParams.put("auto.offset.reset", kafkaReset);
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList(kafkaTopic);

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        System.out.println("start========================================================");
        stream.map(record -> record.value().split("\\|")[kafkaUrlIndex]).print(10);
        System.out.println("over=========================================================");

        ssc.start(); //sparkStream启动程序,开始计算
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
