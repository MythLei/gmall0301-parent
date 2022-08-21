package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Felix
 * @date 2022/8/21
 * 交易域：订单预处理
 * 需要启动的进程
 *      zk、kafka、maxwell、DwdTradeOrderPreProcess
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.4 设置状态的TTL
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15*60+5));
        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDDL("dwd_trade_order_pre_group"));
        //TODO 4.筛选订单明细数据
        Table orderDetail = tableEnv.sqlQuery("select \n" +
            "data['id'] id,\n" +
            "data['order_id'] order_id,\n" +
            "data['sku_id'] sku_id,\n" +
            "data['sku_name'] sku_name,\n" +
            "data['create_time'] create_time,\n" +
            "data['source_id'] source_id,\n" +
            "data['source_type'] source_type,\n" +
            "data['sku_num'] sku_num,\n" +
            "cast(cast(data['sku_num'] as decimal(16,2)) * " +
            "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
            "data['split_total_amount'] split_total_amount,\n" +
            "data['split_activity_amount'] split_activity_amount,\n" +
            "data['split_coupon_amount'] split_coupon_amount,\n" +
            "ts od_ts,\n" +
            "proc_time\n" +
            "from `topic_db` where `table` = 'order_detail' " +
            "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        //TODO 5.筛选订单数据
        Table orderInfo = tableEnv.sqlQuery("select \n" +
            "data['id'] id,\n" +
            "data['user_id'] user_id,\n" +
            "data['province_id'] province_id,\n" +
            "data['operate_time'] operate_time,\n" +
            "data['order_status'] order_status,\n" +
            "`type`,\n" +
            "`old`,\n" +
            "ts oi_ts\n" +
            "from `topic_db`\n" +
            "where `table` = 'order_info'\n" +
            "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info", orderInfo);

        //TODO 6.筛选订单明细活动数据
        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
            "data['order_detail_id'] order_detail_id,\n" +
            "data['activity_id'] activity_id,\n" +
            "data['activity_rule_id'] activity_rule_id\n" +
            "from `topic_db`\n" +
            "where `table` = 'order_detail_activity'\n" +
            "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        //TODO 7.筛选订单明细优惠券数据
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
            "data['order_detail_id'] order_detail_id,\n" +
            "data['coupon_id'] coupon_id\n" +
            "from `topic_db`\n" +
            "where `table` = 'order_detail_coupon'\n" +
            "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        //TODO 8.从MySQL中读取字典表数据
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 9.将上述5张表进行关联
        Table resultTable = tableEnv.sqlQuery("select \n" +
            "od.id,\n" +
            "od.order_id,\n" +
            "od.sku_id,\n" +
            "od.sku_name,\n" +
            "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
            "od.create_time,\n" +
            "od.source_id,\n" +
            "od.source_type,\n" +
            "od.sku_num,\n" +
            "od.split_original_amount,\n" +
            "od.split_activity_amount,\n" +
            "od.split_coupon_amount,\n" +
            "od.split_total_amount,\n" +
            "od.od_ts,\n" +

            "oi.user_id,\n" +
            "oi.province_id,\n" +
            "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
            "oi.operate_time,\n" +
            "oi.order_status,\n" +
            "oi.`type`,\n" +
            "oi.`old`,\n" +
            "oi.oi_ts,\n" +

            "act.activity_id,\n" +
            "act.activity_rule_id,\n" +

            "cou.coupon_id,\n" +

            "dic.dic_name source_type_name,\n" +

            "current_row_timestamp() row_op_ts\n" +
            "from order_detail od \n" +
            "join order_info oi\n" +
            "on od.order_id = oi.id\n" +
            "left join order_detail_activity act\n" +
            "on od.id = act.order_detail_id\n" +
            "left join order_detail_coupon cou\n" +
            "on od.id = cou.order_detail_id\n" +
            "join `base_dic` for system_time as of od.proc_time as dic\n" +
            "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 10.创建动态表和要写入的kafka主题进行映射
        tableEnv.executeSql("create table dwd_trade_order_pre_process(\n" +
            "id string,\n" +
            "order_id string,\n" +
            "sku_id string,\n" +
            "sku_name string,\n" +
            "date_id string,\n" +
            "create_time string,\n" +
            "source_id string,\n" +
            "source_type string,\n" +
            "sku_num string,\n" +
            "split_original_amount string,\n" +
            "split_activity_amount string,\n" +
            "split_coupon_amount string,\n" +
            "split_total_amount string,\n" +
            "od_ts string,\n" +

            "user_id string,\n" +
            "province_id string,\n" +
            "operate_date_id string,\n" +
            "operate_time string,\n" +
            "order_status string,\n" +
            "`type` string,\n" +
            "`old` map<string,string>,\n" +
            "oi_ts string,\n" +

            "activity_id string,\n" +
            "activity_rule_id string,\n" +

            "coupon_id string,\n" +

            "source_type_name string,\n" +

            "row_op_ts timestamp_ltz(3),\n" +
            "primary key(id) not enforced\n" +
            ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        //TODO 11.将关联的结果写到kafka主题中
        tableEnv.executeSql("insert into dwd_trade_order_pre_process \n" +
            "select * from result_table");

    }
}
