package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author Felix
 * @date 2022/8/14
 * 操作kafka的工具类
 */
public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop202:9092,hadoop203:9092,hadoop204:9092";

    //获取消费者对象
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //注意：通过如下方式消费kafka主题数据的时候，如果主题中有空消息，会报错
        // FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record != null && record.value() != null) {
                    return new String(record.value());
                }
                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, props);
        return kafkaConsumer;
    }

    //获取生产者对象
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 15 * 1000 + "");
        //注意：以下这种创建生产者对象的方式  并不能保证生产数据的一致性
        // FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("hadoop202:9092", "dirty_data", new SimpleStringSchema());
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("default_topic",
            new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                    return new ProducerRecord<byte[], byte[]>(topic, jsonStr.getBytes());
                }
            }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return kafkaProducer;
    }

    //获取 从kafka的topic_db主题中读取数据创建动态表的DDL
    public static String getTopicDDL(String groupId) {
        return "CREATE TABLE topic_db (\n" +
            "  `database` string,\n" +
            "  `table` string,\n" +
            "  `type` string,\n" +
            "  `ts` string,\n" +
            "  `data` MAP<string, string>,\n" +
            "  `old` MAP<string, string>,\n" +
            "  proc_time as proctime()\n" +
            ") " + getKafkaDDL("topic_db", groupId);
    }

    //获取kafka连接器相关的连接属性
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = '" + topic + "',\n" +
            "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
            "  'properties.group.id' = '" + groupId + "',\n" +
            "  'scan.startup.mode' = 'group-offsets',\n" +
            "  'format' = 'json'\n" +
            ")";
    }

    //获取upsert-kafka连接器相关的连接属性
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH (\n" +
            "  'connector' = 'upsert-kafka',\n" +
            "  'topic' = '" + topic + "',\n" +
            "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',\n" +
            "  'key.format' = 'json',\n" +
            "  'value.format' = 'json'\n" +
            ")";
    }
}
