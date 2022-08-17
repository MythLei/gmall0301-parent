package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
        //5.1 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = etlDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //5.2 修复标记
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
            new RichMapFunction<JSONObject, JSONObject>() {
                //注意：不能在声明的时候 直接进行初始化  因为获取不到运行时上下文
                private ValueState<String> lastVisitDateState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastVisitDateState
                        = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDateState",String.class));
                }

                @Override
                public JSONObject map(JSONObject jsonObj) throws Exception {
                    //获取新老访客标记值
                    String isNew = jsonObj.getJSONObject("common").getString("is_new");
                    //获取上次访问日期
                    String lastVisitDate = lastVisitDateState.value();
                    //获取当前访问日期
                    Long ts = jsonObj.getLong("ts");
                    String curVisitDate = DateFormatUtil.toDate(ts);

                    if("1".equals(isNew)){
                        if(StringUtils.isEmpty(lastVisitDate)){
                            lastVisitDateState.update(curVisitDate);
                        }else{
                            if(!lastVisitDate.equals(curVisitDate)){
                                isNew = "0";
                                jsonObj.getJSONObject("common").put("is_new",isNew);
                            }
                        }
                    }else{
                        //如果is_new=0,说明当前设备曾经访问过；但是如果状态中没有记录曾经访问日期，我们需要补充一个日期
                        if(StringUtils.isEmpty(lastVisitDate)){
                            String yesterDay = DateFormatUtil.toDate(ts - 3600 * 24 * 1000);
                            lastVisitDateState.update(yesterDay);
                        }
                    }
                    return jsonObj;
                }
            }
        );

        // fixedDS.print(">>>>");
        //TODO 6.使用Flink的侧输出流 对日志进行分流
        //6.1 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        //6.2 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
            new ProcessFunction<JSONObject, String>() {
                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                    //错误日志  放到错误侧输出流
                    JSONObject errJsonObj = jsonObj.getJSONObject("err");
                    if(errJsonObj != null){
                        ctx.output(errTag,jsonObj.toJSONString());
                        jsonObj.remove("err");
                    }

                    JSONObject startJsonObj = jsonObj.getJSONObject("start");
                    if(startJsonObj != null){
                        //启动日志  放到启动侧输出流
                        ctx.output(startTag,jsonObj.toJSONString());
                    }else{
                        //页面日志
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        Long ts = jsonObj.getLong("ts");

                        //曝光日志
                        JSONArray displayArr = jsonObj.getJSONArray("displays");
                        if(displayArr != null && displayArr.size() > 0){
                            //说明页面有曝光信息  对曝光数据进行遍历
                            for (int i = 0; i < displayArr.size(); i++) {
                                JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                JSONObject displayNewObj = new JSONObject();
                                displayNewObj.put("common",commonJsonObj);
                                displayNewObj.put("page",pageJsonObj);
                                displayNewObj.put("display",displayJsonObj);
                                displayNewObj.put("ts",ts);
                                // 放到曝光侧输出流
                                ctx.output(displayTag,displayNewObj.toJSONString());
                            }
                        }


                        //动作日志
                        JSONArray actionArr = jsonObj.getJSONArray("actions");
                        if(actionArr != null && actionArr.size() > 0){
                            //说明在当前页面上有动作
                            for (int i = 0; i < actionArr.size(); i++) {
                                JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                JSONObject actionNewObj = new JSONObject();
                                actionNewObj.put("common",commonJsonObj);
                                actionNewObj.put("page",pageJsonObj);
                                actionNewObj.put("action",actionJsonObj);
                                // 放到动作侧输出流
                                ctx.output(actionTag,actionNewObj.toJSONString());
                            }
                        }

                        //将页面上曝光以及动作删除掉 将日志放到主流
                        jsonObj.remove("displays");
                        jsonObj.remove("actions");
                        out.collect(jsonObj.toJSONString());
                    }
                }
            }
        );

        //TODO 7.将不同流的数据写到kafka的不同主题中
        DataStream<String> errDS = pageDS.getSideOutput(errTag);
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print(">>>>");
        startDS.print("###");
        displayDS.print("~~~");
        actionDS.print("$$$");
        errDS.print("&&&");

        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_display_log"));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_action_log"));
        errDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_err_log"));

        env.execute();
    }
}