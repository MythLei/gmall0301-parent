package com.atguigu.gmall;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2022/8/14
 */
public class TestFlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop202")
            .port(3306)
            .databaseList("gmall0301_config")
            .tableList("gmall0301_config.t_user")
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.initial())
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
