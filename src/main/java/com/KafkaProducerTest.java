package com;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author Alan Chen
 * @description
 * @date 2020/9/21
 */
public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {

        /**
         * 1、准备配置信息
         */
        Properties props = new Properties();

        //定义Kafka服务器地址列表，不需要指定所有的broker
        props.put("bootstrap.servers", "47.105.146.74:9092,39.108.250.186:9092");

        //生产者需要leader确认请求完成之前接收的应答数
        props.put("acks", "-1");

        //客户端失败重试次数
        props.put("retries",1);

        //生产者打包消息的批量大小，以字节为单位，此次是16k
        props.put("batch.size",16384);

        //生产者延迟1ms发送消息
        props.put("linger.ms",1);

        //生产者缓存内存的大小，以字节为单位，此处是32m
        props.put("buffer.memory",33554432);

        // key 序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // value 序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /**
         * 2、创建生产者对象
         */
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        /**
         * 3、通过生产者发送消息
         *
         */

        /**
         * 发送消息的三种方式：
         *
         * - 同步阻塞发送
         * 适用场景：发送消息不能出错，消息的顺序不能乱，不关心高吞吐量。
         *
         * - 异步发送（发送并忘记）
         * 适用场景：发送消息不管会不会出错，消息的顺序乱了没有关系，关心高吞吐量。
         *
         * - 异步发送（进行回调处理）
         * 适用场景：发送消息不能出错，但我不关心消息的具体顺序。
         */

        // 1、同步阻塞发送
        //sync(producer);

        // 2、异步发送（发送并忘记）
        //async1(producer);

        // 3、异步发送（回调函数）
        async2(producer);

    }

    /**
     * 同步阻塞发送
     * @param producer
     * @throws Exception
     */
    private static void sync(KafkaProducer producer) throws Exception {
        System.out.println("同步发送消息start......");

        ProducerRecord<String, String> record = new ProducerRecord<>("alanchen_topic",0,"key-sync","同步发送消息");
        Future<RecordMetadata> send = producer.send(record);
        RecordMetadata recordMetadata = send.get();
        producer.flush();

        System.out.println(recordMetadata);
        System.out.println("同步发送消息end......");


    }

    /**
     * 异步发送（发送并忘记）
     * @param producer
     * @throws Exception
     */
    private static void async1(KafkaProducer producer){
        System.out.println("异步发送（发送并忘记）start......");

        ProducerRecord<String, String> record = new ProducerRecord<>("alanchen_topic",0,"key-async1","异步发送（发送并忘记）");
        producer.send(record);
        producer.flush();

        System.out.println("异步发送（发送并忘记）end......");
    }

    /**
     * 异步发送（回调函数）
     * @param producer
     * @throws Exception
     */
    private static void async2(KafkaProducer producer){
        System.out.println("异步发送（回调函数）start......");

        ProducerRecord<String, String> record = new ProducerRecord<>("alanchen_topic",0,"key-async2","异步发送（回调函数）");
        producer.send(record,new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println("异步发送消息成功："+metadata);
                exception.printStackTrace();
            }
        });
        producer.flush();

        System.out.println("异步发送（回调函数）end......");
    }
}
