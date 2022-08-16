package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Felix
 * @date 2022/8/14
 * 维度层DIM处理应用
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、DimApp
 * 总结：
 *      整个维度处理流程图  必须掌握
 *      开发流程：
 *          基本环境准备
 *          检查点设置
 *          从kafka的topic_db主题中读取业务数据
 *          对读取的数据进行类型转换    jsonStr->jsonObj
 *          简单的ETL
 *          ~~~~~~~~~~~~~~~~~~~~主流~~~~~~~~~~~~~~~~~~~~~~~~
 *          使用FlinkCDC读取配置表数据
 *          广播配置流
 *          ~~~~~~~~~~~~~~~~~~~~广播流~~~~~~~~~~~~~~~~~~~~~~~~
 *          将主流和广播流进行关联 -- connect
 *          对关联之后的数据进行处理--process
 *              class TableProcessFunction extends BroadcastProcessFunction{
 *                  processElement---处理主流数据
 *                  processBroadcastElement---处理广播流数据
 *                      //将flinkCDC读取到的json字符串转换为jsonObj
 *                      //判断对配置表进行操作类型
 *                      删除:
 *                          从广播状态中将对应的配置删除掉
 *                      其它:
 *                          将配置信息放到广播状态中或者更新广播状态的配置
 *              }
 *
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 job取消后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/gmall/ck");
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3.从kafka的topic_db主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "topic_db";
        String groupId = "dim_app_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic,groupId);
        //3.3 消费数据  封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        //TODO 4.对读取的数据 进行类型转换      jsonStr->jsonObj
        /*//匿名内部类
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                return JSON.parseObject(jsonStr);
            }
        });
        //lambda表达式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(jsonStr -> JSON.parseObject(jsonStr));
        */
        //方法的默认调用
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // jsonObjDS.print(">>>");

        //TODO 5.对流中的数据进行简单的ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                try {
                    jsonObj.getJSONObject("data");
                    if (jsonObj.getString("type").equals("bootstrap-start")
                        || jsonObj.getString("type").equals("bootstrap-complete")) {
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        });
        //TODO 6.使用FlinkCDC读取配置表
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop202")
            .port(3306)
            .databaseList("gmall0301_config")
            .tableList("gmall0301_config.table_process")
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.initial())
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        DataStreamSource<String> mySqlDS = env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //TODO 7.将读取到的配置信息进行广播
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
            = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor",String.class,TableProcess.class);
        BroadcastStream<String> broadcastDS = mySqlDS.broadcast(mapStateDescriptor);

        //TODO 8.将主流和广播流进行关联
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        //TODO 9.对关联之后的数据 进行处理
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(
            new TableProcessFunction(mapStateDescriptor)
        );
        //TODO 10.将维度数据写到Phoenix表中
        env.execute();
    }
}