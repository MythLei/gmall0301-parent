package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
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
 * 开发流程
 *      基本环境准备
 *      检查点相关的设置
 *      从kafka的topic_db主题中读取业务数据
 *      对读取的数据进行类型转换        jsonStr->jsonObj
 *      简单ETL
 *      使用FlinkCDC读取配置表---配置流
 *      将配置流进行广播，声明广播状态---广播流
 *      将主流和广播流进行关联  ---connect
 *      对关联后的数据进行处理  ---process
 *      class TableProcessFunction extends BroadcastProcessFunction{
 *          processElement---处理主流数据
 *              获取处理业务数据库表名
 *              获取广播状态
 *              根据表名到广播状态中获取对应的配置信息
 *              如果能够获取对应的配置信息，说明是维度数据
 *                  对不需要传递的属性进行过滤
 *                  补充传递的目的地
 *                  将处理的维度的data部分向下游传递
 *          processBroadcastElement---处理广播流数据
 *              op="d"：对配置表进行了删除
 *                  将删除的配置表从广播状态中删除掉
 *              除了删除以外的其它操作
 *                  提前将维度表创建出来
 *                      拼接建表语句
 *                      使用JDBC执行建表语句
 *                  将配置表中的配置信息添加或者更新到广播状态中
 *      }
 *      将维度数据写到phoenix表中
 *          class DimSinkFunction extends RichSinkFunction{
 *              invoke(){
 *                  拼接upsert语句
 *                  使用JDBC执行upsert语句
 *              }
 *          }
 * 以历史维度数据处理为例，分析程序执行的思路
 *      启动zk、kafka、maxwell、hdfs、hbase、DimApp进程
 *      当DimApp应用启动的时候，首先会读取配置表中的数据，将其放到广播状态中
 *      当执行mysql_to_kafka_init.sh all脚本的时候，maxwell-bootstrap会扫描业务数据库中的所有维度表
 *      将扫描到的维度数据交给maxwell进行处理
 *      maxwell会将维度数据封装为json字符串，发送到kafka的topic_db主题中
 *      DimApp从topic_db主题中读取数据
 *      在TableProcessFunction类中，有processElement方法处理读取到的数据
 *      判断是否是维度
 *      如果是维度，向下游传递
 *      写到phoenix表中
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
        dimDS.print(">>>>");
        //TODO 10.将维度数据写到Phoenix表中
        dimDS.addSink(new DimSinkFunction());

        env.execute();
    }
}