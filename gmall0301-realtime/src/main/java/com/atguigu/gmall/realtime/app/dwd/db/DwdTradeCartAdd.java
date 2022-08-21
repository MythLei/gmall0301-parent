package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2022/8/20
 * 交易域：加购事实表
 * 需要启动的进程
 *      zk、kafka、maxwell、DwdTradeCartAdd
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 并行度设置
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关设置(略)
        //TODO 3.从topic_db主题中读取业务表变化数据  创建动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDDL("dwd_trade_cart_add_group"));

        // tableEnv.executeSql("select * from topic_db").print();

        //TODO 4.从业务数据中筛选出加购数据
        Table cartAdd = tableEnv.sqlQuery("select\n" +
            "    data['id'] id,\n" +
            "    data['user_id'] user_id,\n" +
            "    data['sku_id'] sku_id,\n" +
            "    data['source_type'] source_type,\n" +
            "    if(`type`='insert',data['sku_num'],cast((CAST(data['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) as string)\n" +
            "    ) sku_num,\n" +
            "    ts,\n" +
            "    proc_time\n" +
            "from topic_db where `table`='cart_info' and\n" +
            "    (`type` = 'insert'  or (`type`='update' and `old`['sku_num'] is not null and (CAST(data['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT))))");
        tableEnv.createTemporaryView("cart_add",cartAdd);
        // tableEnv.executeSql("select * from cart_add").print();

        //TODO 5.从MySQL中读取字典维度表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //TODO 6.关联加购和字典表
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
            " cadd.id,\n" +
            " cadd.user_id,\n" +
            " cadd.sku_id,\n" +
            " cadd.source_type source_type_code,\n" +
            " dic.dic_name source_type_name,\n" +
            " cadd.sku_num,\n" +
            " cadd.ts\n" +
            "FROM cart_add AS cadd JOIN base_dic FOR SYSTEM_TIME AS OF cadd.proc_time AS dic  " +
            "ON cadd.source_type = dic.dic_code");
        tableEnv.createTemporaryView("res_table",resTable);

        // tableEnv.executeSql("select * from res_table").print();


        //TODO 7.将关联的结果写到kafka主题中
        //7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE dwd_trade_cart_add (\n" +
            "  id string,\n" +
            "  user_id string,\n" +
            "  sku_id string,\n" +
            "  source_type_code string,\n" +
            "  source_type_name string,\n" +
            "  sku_num string,\n" +
            "  ts string,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add"));
        //7.2 写入
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from res_table");
    }
}
