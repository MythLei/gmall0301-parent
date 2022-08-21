package com.atguigu.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Felix
 * @date 2022/8/21
 * 交易域：支付成功事务事实表
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
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15*60+5));
        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表
        //TODO 4.过滤出支付成功数据
        //TODO 5.从下单表中读取下单数据
        //TODO 6.从MySQL中读取字典表数据
        //TODO 7.将3张表进行连接
        //TODO 8.创建动态表和要写入的kafka主题进行映射
        //TODO 9.将连接的结果写到kafka主题中
    }
}
