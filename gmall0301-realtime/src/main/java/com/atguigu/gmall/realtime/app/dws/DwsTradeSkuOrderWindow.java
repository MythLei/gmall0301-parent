package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;

/**
 * @author Felix
 * @date 2022/8/27
 * 交易域：sku粒度下单相关聚合统计
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、
 *      DwdTradeOrderPreProcess、DwdTradeOrderDetail、DwsTradeSkuOrderWindow
 *
 * 开发流程
 *      基本环境准备
 *      检查点设置
 *      从dwd的下单事实表中获取下单数据
 *      对读取的数据进行类型转换    jsonStr->jsonObj
 *      按照订单明细的id进行分组
 *      对数据进行去重
 *              状态  + 定时器
 *      对流中的数据进行类型转换    jsonObj->实体类
 *      按照user_id进行分组
 *      判断是否为下单独立用户
 *      指定Watermark以及提取事件时间字段
 *      按照sku_id进行分组
 *      开窗
 *      聚合统计
 *      和维度进行关联
 *          基本的维度关联
 *              在PhoenixUtil工具类中，提供了一个方法
 *                  List<T> queryList(conn,sql,Class<T>)
 *              封装DimUtil工具类
 *                  JSONObject getDimInfoNoCache(conn,tableName,Tuple2... params)
 *              在主程序中，实现维度关联
 *                  -获取维度的主键
 *                  -根据主键获取维度对象
 *                  -将维度对象的属性 补充到流中的对象属性上
 *          优化1：旁路缓存
 *          优化2：异步IO
 *
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 设置流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置(略)

        //TODO 3.从kafka的dwd_trade_order_detail主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_sku_order_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        //TODO 4.对读取的数据进行类型转换       jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // {"create_time":"2022-08-13 09:39:22","sku_num":"1","split_original_amount":"5499.0000",
        // "sku_id":"35","date_id":"2022-08-13","source_type_name":"用户查询","user_id":"198",
        // "province_id":"13","source_type_code":"2401","row_op_ts":"2022-08-27 01:39:17.933Z",
        // "sku_name":"华为智慧屏V 星际黑","id":"242","order_id":"105","split_total_amount":"5499.0",
        // "ts":"1661564362"}
        // jsonObjDS.print(">>>>");
        //TODO 5.按照订单明细id进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //TODO 6.去除重复数据
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
            new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                private ValueState<JSONObject> lastJsonObjState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastJsonObjState
                        = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class));
                }

                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject lastJsonObj = lastJsonObjState.value();
                    if (lastJsonObj == null) {
                        lastJsonObjState.update(jsonObj);
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                    } else {
                        // row_op_ts 2022-08-27 01:39:17.933Z
                        // row_op_ts 2022-08-27 01:39:17.93Z
                        String lastRowOpTs = lastJsonObj.getString("row_op_ts");
                        String curRowOpTs = jsonObj.getString("row_op_ts");
                        if (TimestampLtz3CompareUtil.compare(lastRowOpTs, curRowOpTs) <= 0) {
                            lastJsonObjState.update(jsonObj);
                        }
                    }
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject lastJsonObj = lastJsonObjState.value();
                    if (lastJsonObj != null) {
                        out.collect(lastJsonObj);
                        lastJsonObjState.clear();
                    }
                }
            }
        );

        //TODO 7.对流中数据进行类型转换        jsonObj ->实体类
        SingleOutputStreamOperator<TradeSkuOrderBean> orderBeanDS = distinctDS.map(
            new MapFunction<JSONObject, TradeSkuOrderBean>() {
                @Override
                public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                    // {"create_time":"2022-08-13 09:39:22","sku_num":"1","split_original_amount":"5499.0000",
                    // "sku_id":"35","date_id":"2022-08-13","source_type_name":"用户查询","user_id":"198",
                    // "province_id":"13","source_type_code":"2401","row_op_ts":"2022-08-27 01:39:17.933Z",
                    // "sku_name":"华为智慧屏V 星际黑","id":"242","order_id":"105","split_total_amount":"5499.0",
                    // "ts":"1661564362"}
                    String orderId = jsonObj.getString("order_id");
                    String userId = jsonObj.getString("user_id");
                    String skuId = jsonObj.getString("sku_id");
                    Double splitOriginalAmount = jsonObj.getDouble("split_original_amount");
                    Double splitActivityAmount = jsonObj.getDouble("split_activity_amount");
                    Double splitCouponAmount = jsonObj.getDouble("split_coupon_amount");
                    Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                    Long ts = jsonObj.getLong("ts") * 1000L;
                    TradeSkuOrderBean trademarkCategoryUserOrderBean = TradeSkuOrderBean.builder()
                        .orderIdSet(new HashSet<String>(
                            Collections.singleton(orderId)
                        ))
                        .skuId(skuId)
                        .userId(userId)
                        .orderUuCount(0L)
                        .originalAmount(splitOriginalAmount)
                        .activityAmount(splitActivityAmount == null ? 0.0 : splitActivityAmount)
                        .couponAmount(splitCouponAmount == null ? 0.0 : splitCouponAmount)
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                    return trademarkCategoryUserOrderBean;
                }
            }
        );
        //TODO 8.按照用户id进行分组
        KeyedStream<TradeSkuOrderBean, String> userIdKeyedDS = orderBeanDS.keyBy(TradeSkuOrderBean::getUserId);
        //TODO 9.使用Flink状态编程  判断是否为独立用户
        SingleOutputStreamOperator<TradeSkuOrderBean> uuDS = userIdKeyedDS.process(
            new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {
                private ValueState<String> lastOrderDateState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<String>("lastOrderDateState", String.class);
                    valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    lastOrderDateState = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public void processElement(TradeSkuOrderBean orderBean, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                    String lastOrderDate = lastOrderDateState.value();
                    String curOrderDate = DateFormatUtil.toDate(orderBean.getTs());
                    if (StringUtils.isEmpty(lastOrderDate) || !lastOrderDate.equals(curOrderDate)) {
                        orderBean.setOrderUuCount(1L);
                        lastOrderDateState.update(curOrderDate);
                    }
                    out.collect(orderBean);
                }
            }
        );
        //TODO 10.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TradeSkuOrderBean> withWatermarkDS = uuDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                        @Override
                        public long extractTimestamp(TradeSkuOrderBean orderBean, long recordTimestamp) {
                            return orderBean.getTs();
                        }
                    }
                )
        );
        //TODO 11.按照sku维度进行分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = withWatermarkDS.keyBy(TradeSkuOrderBean::getSkuId);

        //TODO 12.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 13.聚合计算
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
            new ReduceFunction<TradeSkuOrderBean>() {
                @Override
                public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                    value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                    value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                    value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                    value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                    value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                    value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                    return value1;

                }
            },
            new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                @Override
                public void process(String s, Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                    for (TradeSkuOrderBean orderBean : elements) {
                        orderBean.setStt(DateFormatUtil.toYmdHms(context.window().getStart()));
                        orderBean.setEdt(DateFormatUtil.toYmdHms(context.window().getEnd()));
                        orderBean.setTs(System.currentTimeMillis());
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
            }
        );

        // TradeSkuOrderBean(stt=2022-08-27 15:25:40, edt=2022-08-27 15:25:50,
        // trademarkId=null, trademarkName=null, category1Id=null, category1Name=null,
        // category2Id=null, category2Name=null, category3Id=null, category3Name=null,
        // orderIdSet=[183], userId=47, skuId=1, skuName=null, spuId=null, spuName=null,
        // orderUuCount=0, orderCount=1, originalAmount=17997.0, activityAmount=0.0,
        // couponAmount=0.0, orderAmount=17997.0, ts=1661585181829)
        // reduceDS.print(">>>>");

        //TODO 14.和SKU维度进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
            new MapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                @Override
                public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                    //获取当前处理下单数据的商品的sku_id
                    String skuId = orderBean.getSkuId();
                    //根据商品的skuId到商品维度表中查询出对应的维度数据
                    DruidDataSource dataSource = DruidDSUtil.createDataSource();
                    Connection conn = dataSource.getConnection();
                    // ID,SPU_ID,PRICE,SKU_NAME,SKU_DESC,WEIGHT,TM_ID,CATEGORY3_ID,SKU_DEFAULT_IMG,IS_SALE,CREATE_TIME
                    JSONObject dimInfoJsonObj = DimUtil.getDimInfoNoCache(conn, "dim_sku_info", Tuple2.of("id", skuId));
                    //将维度的属性  补充到流中对象的
                    orderBean.setSkuName(dimInfoJsonObj.getString("SKU_NAME"));
                    orderBean.setSpuId(dimInfoJsonObj.getString("SPU_ID"));
                    orderBean.setTrademarkId(dimInfoJsonObj.getString("TM_ID"));
                    orderBean.setCategory3Id(dimInfoJsonObj.getString("CATEGORY3_ID"));
                    return orderBean;
                }
            }
        );

        withSkuInfoDS.print(">>>>");

        //TODO 15.和SPU维度进行关联
        //TODO 16.和TM维度进行关联
        //TODO 17.和Category3维度进行关联
        //TODO 18.和Category2维度进行关联
        //TODO 19.和Category1维度进行关联
        //TODO 20.将关联聚合结果写到Clickhouse表中

        env.execute();
    }
}
