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
 * 交易域：支付成功事务事实表
 * 需要启动的进程
 *      zk、kafka、maxwell、DwdTradeOrderPreProcess、
 *      DwdTradeOrderDetail、DwdTradePayDetailSuc
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.4 设置状态的TTL
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));
        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDDL("dwd_trade_pay_suc_group"));
        //TODO 4.过滤出支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("select\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['payment_type'] payment_type,\n" +
                "data['callback_time'] callback_time,\n" +
                "`proc_time`,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'payment_info'\n"
//                +
//                "and `type` = 'update'\n" +
//                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        //TODO 5.从下单表中读取下单数据
        tableEnv.executeSql("" +
            "create table dwd_trade_order_detail(\n" +
            "id string,\n" +
            "order_id string,\n" +
            "user_id string,\n" +
            "sku_id string,\n" +
            "sku_name string,\n" +
            "province_id string,\n" +
            "activity_id string,\n" +
            "activity_rule_id string,\n" +
            "coupon_id string,\n" +
            "date_id string,\n" +
            "create_time string,\n" +
            "source_id string,\n" +
            "source_type_code string,\n" +
            "source_type_name string,\n" +
            "sku_num string,\n" +
            "split_original_amount string,\n" +
            "split_activity_amount string,\n" +
            "split_coupon_amount string,\n" +
            "split_total_amount string,\n" +
            "ts string,\n" +
            "row_op_ts timestamp_ltz(3)\n" +
            ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_pay_detail_suc"));

        //TODO 6.从MySQL中读取字典表数据
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 7.将3张表进行连接
        Table resultTable = tableEnv.sqlQuery("" +
            "select\n" +
            "od.id order_detail_id,\n" +
            "od.order_id,\n" +
            "od.user_id,\n" +
            "od.sku_id,\n" +
            "od.sku_name,\n" +
            "od.province_id,\n" +
            "od.activity_id,\n" +
            "od.activity_rule_id,\n" +
            "od.coupon_id,\n" +
            "pi.payment_type payment_type_code,\n" +
            "dic.dic_name payment_type_name,\n" +
            "pi.callback_time,\n" +
            "od.source_id,\n" +
            "od.source_type_code,\n" +
            "od.source_type_name,\n" +
            "od.sku_num,\n" +
            "od.split_original_amount,\n" +
            "od.split_activity_amount,\n" +
            "od.split_coupon_amount,\n" +
            "od.split_total_amount split_payment_amount,\n" +
            "pi.ts,\n" +
            "od.row_op_ts row_op_ts\n" +
            "from payment_info pi \n" +
            "join dwd_trade_order_detail od \n" +
            "on pi.order_id = od.order_id\n" +
            " join `base_dic` for system_time as of pi.proc_time as dic\n" +
            "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 8.创建动态表和要写入的kafka主题进行映射
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc(\n" +
            "order_detail_id string,\n" +
            "order_id string,\n" +
            "user_id string,\n" +
            "sku_id string,\n" +
            "sku_name string,\n" +
            "province_id string,\n" +
            "activity_id string,\n" +
            "activity_rule_id string,\n" +
            "coupon_id string,\n" +
            "payment_type_code string,\n" +
            "payment_type_name string,\n" +
            "callback_time string,\n" +
            "source_id string,\n" +
            "source_type_code string,\n" +
            "source_type_name string,\n" +
            "sku_num string,\n" +
            "split_original_amount string,\n" +
            "split_activity_amount string,\n" +
            "split_coupon_amount string,\n" +
            "split_payment_amount string,\n" +
            "ts string,\n" +
            "row_op_ts timestamp_ltz(3),\n" +
            "primary key(order_detail_id) not enforced\n" +
            ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        //TODO 9.将连接的结果写到kafka主题中
        tableEnv.executeSql("" +
            "insert into dwd_trade_pay_detail_suc select * from result_table");

    }
}
