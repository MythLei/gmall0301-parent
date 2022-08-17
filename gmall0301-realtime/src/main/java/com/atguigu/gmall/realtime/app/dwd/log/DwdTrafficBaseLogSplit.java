package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2022/8/17
 * 流量域：未经加工事实表处理(日志分流)
 * 需要启动的进程
 *      flume、zk、kafka、DwdTrafficBaseLogSplit
 */
public class DwdTrafficBaseLogSplit {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        /*
        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/xxxx");
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 3.从kafka的topic_log主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "topic_log";
        String groupId = "dwd_traffic_log_split_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        // kafkaStrDS.print(">>>>>");

        //TODO 4.对读取的数据进行类型转换 并进行简单的ETL 将脏数据放到侧输出流 输出到kafka主题
        //4.1 定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        //4.2 类型转换以及ETL
        SingleOutputStreamOperator<JSONObject> etlDS = kafkaStrDS.process(
            new ProcessFunction<String, JSONObject>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                    try {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    } catch (Exception e) {
                        //如果发生异常，说明jsonstr不是标准的json字符串  属于脏数据，放到侧输出流中
                        ctx.output(dirtyTag,jsonStr);
                    }
                }
            }
        );
        //4.3 获取侧输出流
        // etlDS.print(">>>>");写到kafka主题中
        DataStream<String> dirtyDS = etlDS.getSideOutput(dirtyTag);
        // dirtyDS.print("####");

        dirtyDS.addSink(MyKafkaUtil.getKafkaProducer("dirty_data"));

        //TODO 5.使用Flink的状态编程  修复新老访客标记

        //TODO 6.使用Flink的侧输出流 对日志进行分流

        //TODO 7.将不同流的数据写到kafka的不同主题中


        env.execute();
    }
}
