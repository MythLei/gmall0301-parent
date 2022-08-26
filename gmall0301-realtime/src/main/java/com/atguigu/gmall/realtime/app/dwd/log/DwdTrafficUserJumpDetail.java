package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author Felix
 * @date 2022/8/19
 * 流量域：用户跳出事实表
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka的page_log主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 封装流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
        /*// 测试数据
        DataStream<String> kafkaStrDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );*/


        //TODO 4.对读取的数据进行类型的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //TODO 5.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
            // WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<JSONObject>() {
                        @Override
                        public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                            return jsonObj.getLong("ts");
                        }
                    }
                )
        );

        //TODO 6.按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 7.使用FlinkCEP判断是否为跳出行为
        //7.1 定义pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
            new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObj) {
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    return StringUtils.isEmpty(lastPageId);
                }
            }
        ).next("second").where(
            new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObj) {
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    return StringUtils.isEmpty(lastPageId);
                }
            }
        ).within(Time.seconds(10));
        //7.2 将pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        //7.3 从流中提取数据(完全匹配  + 超时)
        OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag") {};
        SingleOutputStreamOperator<String> matchDS = patternDS.select(
            timeoutTag,
            new PatternTimeoutFunction<JSONObject, String>() {
                @Override
                public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                    return map.get("first").get(0).toJSONString();
                }
            },
            new PatternSelectFunction<JSONObject, String>() {
                @Override
                public String select(Map<String, List<JSONObject>> map) throws Exception {
                    return map.get("first").get(0).toJSONString();
                }
            }
        );

        //TODO 8.将两条流数据进行合并
        DataStream<String> timeoutDS = matchDS.getSideOutput(timeoutTag);
        DataStream<String> unionDS = matchDS.union(timeoutDS);
        unionDS.print(">>>");

        //TODO 9.将跳出数据写到kafka的主题中
        unionDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_user_jump_detail"));
        env.execute();

    }
}