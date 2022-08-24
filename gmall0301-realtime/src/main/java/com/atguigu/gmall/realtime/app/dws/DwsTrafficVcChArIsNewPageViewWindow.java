package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Felix
 * @date 2022/8/24
 * 流量域：版本、地区、渠道、新老访客维度聚合统计
 * 需要启动的进程
 *      zk、kafka、flume、DwdTrafficBaseLogSplit、DwdTrafficUniqueVisitorDetail
 *      DwdTrafficUserJumpDetail、DwsTrafficVcChArIsNewPageViewWindow
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置(略)

        //TODO 3.从kafka的主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String pageLogTopic = "dwd_traffic_page_log";
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujdTopic = "dwd_traffic_user_jump_detail";
        String groupId = "dws_traffic_vc_ch_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageLogKafkaConsumer = MyKafkaUtil.getKafkaConsumer(pageLogTopic, groupId);
        FlinkKafkaConsumer<String> uvKafkaConsumer = MyKafkaUtil.getKafkaConsumer(uvTopic, groupId);
        FlinkKafkaConsumer<String> ujdKafkaConsumer = MyKafkaUtil.getKafkaConsumer(ujdTopic, groupId);
        //3.3 消费数据  封装为流
        DataStreamSource<String> pageLogStrDS = env.addSource(pageLogKafkaConsumer);
        DataStreamSource<String> uvStrDS = env.addSource(uvKafkaConsumer);
        DataStreamSource<String> ujdStrDS = env.addSource(ujdKafkaConsumer);

        //TODO 4.对读取的数据进行类型转换       jsonStr->实体类对象
        //pageLog
        SingleOutputStreamOperator<TrafficPageViewBean> pageLogStatsDS = pageLogStrDS.map(
            new MapFunction<String, TrafficPageViewBean>() {
                @Override
                public TrafficPageViewBean map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    Long ts = jsonObj.getLong("ts");

                    TrafficPageViewBean pageViewBean = new TrafficPageViewBean(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        0L,
                        1L,
                        pageJsonObj.getLong("during_time"),
                        0L,
                        ts
                    );
                    String lastPageId = pageJsonObj.getString("last_page_id");
                    if (StringUtils.isEmpty(lastPageId)) {
                        pageViewBean.setSvCt(1L);
                    }
                    return pageViewBean;
                }
            }
        );
        //uv
        SingleOutputStreamOperator<TrafficPageViewBean> uvStatsDS = uvStrDS.map(
            new MapFunction<String, TrafficPageViewBean>() {
                @Override
                public TrafficPageViewBean map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");
                    TrafficPageViewBean pageViewBean = new TrafficPageViewBean(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        1L, 0L, 0L, 0L, 0L, ts
                    );
                    return pageViewBean;
                }
            }
        );
        //ujd
        SingleOutputStreamOperator<TrafficPageViewBean> ujdStatsDS = ujdStrDS.map(
            new MapFunction<String, TrafficPageViewBean>() {
                @Override
                public TrafficPageViewBean map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");
                    TrafficPageViewBean pageViewBean = new TrafficPageViewBean(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L, 0L, 0L, 0L, 1L, ts
                    );
                    return pageViewBean;
                }
            }
        );

        //TODO 5.使用union对上述流进行合并
        DataStream<TrafficPageViewBean> unionDS = pageLogStatsDS.union(
            uvStatsDS,
            ujdStatsDS
        );

        unionDS.print(">>>>>");

        //TODO 6.指定Watermark以及提取事件时间字段

        //TODO 7.按照统计维度分组

        //TODO 8.开窗

        //TODO 9.聚合计算

        //TODO 10.将聚合的结果写到Clickhouse表中
        env.execute();
    }
}
