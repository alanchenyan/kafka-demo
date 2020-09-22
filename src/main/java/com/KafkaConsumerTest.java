package com;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author Alan Chen
 * @description
 * @date 2020/9/22
 */
public class KafkaConsumerTest {

    public static void main(String[] args) {
        /**
         * 1、准备配置信息
         */
        Properties props = new Properties();

        //定义Kafka服务器地址列表，不需要指定所有的broker
        props.put("bootstrap.servers", "47.105.146.74:9092,39.108.250.186:9092");

        //消费者组ID
        props.put("group.id", "ac");

        //是否自动确认offset
        props.put("enable.auto.commit","true");

        //自动确认offset时间间隔
        props.put("auto.commit.interval.ms",1000);

        //生产者延迟1ms发送消息
        props.put("linger.ms",1);

        //生产者缓存内存的大小，以字节为单位，此处是32m
        props.put("buffer.memory",33554432);

        // key 序列化类
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // value 序列化类
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 2、创建一个消费者
         */
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        /**
         * 3、指定消费哪一个topic的数据
         */
        //指定分区消费
        TopicPartition partition = new TopicPartition("alanchen_topic",0);

        //获取已经提交的偏移量
        long offset =0L;
        OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
        if(offsetAndMetadata!=null){
            offset = offsetAndMetadata.offset();
        }
        System.out.println("当前消费的偏移量："+offset);

        // 指定偏移量消费
        consumer.assign(Arrays.asList(partition));
        consumer.seek(partition,offset);

        //循环拉取数据
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(1000);
            for(ConsumerRecord<String,String> record : records){
                System.out.println("消费的数据为："+record.value());
            }
        }

    }
}
