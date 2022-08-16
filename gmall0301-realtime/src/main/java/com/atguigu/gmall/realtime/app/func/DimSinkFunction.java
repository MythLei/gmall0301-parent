package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

/**
 * @author Felix
 * @date 2022/8/16
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource dataSource;
    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //jsonObj >>>>:4> {"tm_name":"cls","sink_table":"dim_base_trademark","id":12}
        //获取目的地表名
        String sinkTable = jsonObj.getString("sink_table");
        // {"tm_name":"cls","id":12}
        jsonObj.remove("sink_table");

        //拼接upsert语句
        String upsertSql = "upsert into "+ GmallConfig.PHOENIX_SCHEMA +"."+sinkTable +
            " ("+StringUtils.join(jsonObj.keySet(),",")+") " +
            " values" +
            " ('"+StringUtils.join(jsonObj.values(),"','")+"')";

        System.out.println("向phoenix表中插入数据的SQL:" + upsertSql);

        Connection conn = dataSource.getConnection();
        PhoenixUtil.executeSql(conn,upsertSql);
    }
}
