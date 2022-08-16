package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Felix
 * @date 2022/8/14
 * 对主流和广播流关联后的数据进行处理
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private DruidDataSource dataSource;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
    }

    //处理主流业务数据
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

    }

    //{"before":null,"after":{"source_table":"base_region","sink_table":"dim_base_region","sink_columns":"id,region_name","sink_pk":null,"sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1660463086411,"snapshot":"false","db":"gmall0301_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1660463086412,"transaction":null}
    //处理广播流配置数据
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        //为了处理属性方便  将json字符串转换为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        //获取对配置表的操作类型
        String op = jsonObj.getString("op");

        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if("d".equals(op)){
            //从广播状态中将对应的配置信息删除掉
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            broadcastState.remove(before.getSourceTable());
        }else{
            //将配置信息放到广播状态中 或者更新广播状态中的配置
            TableProcess after = jsonObj.getObject("after", TableProcess.class);
            //获取业务库中维度表名称
            String sourceTable = after.getSourceTable();
            //获取输出目的地
            String sinkTable = after.getSinkTable();
            //获取表中字段
            String sinkColumns = after.getSinkColumns();
            //获取建表主键
            String sinkPk = after.getSinkPk();
            //获取建表扩展
            String sinkExtend = after.getSinkExtend();

            //提前创建维度表
            checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);

            //将读取到的配置信息放到广播状态中
            broadcastState.put(sourceTable,after);
        }

    }

    //phoenix中维度表的创建
    private void checkTable(String tableName, String sinkColumns, String pk, String ext) {
        //空值处理
        if(pk == null){
            pk = "id";
        }
        if(ext == null){
            ext = "";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists "+ GmallConfig.PHOENIX_SCHEMA +"."+tableName+"(");
        //处理表中字段
        String[] columnArr = sinkColumns.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String column = columnArr[i];
            if(column.equals(pk)){
                createSql.append(column + " varchar primary key");
            }else{
                createSql.append(column + " varchar");
            }

            if(i < columnArr.length - 1){
                createSql.append(",");
            }
        }
        createSql.append(") " + ext);
        System.out.println("在phoenix中执行的建表语句:" + createSql);

        PreparedStatement ps = null;
        Connection conn = null;
        try {
            //获取连接
            conn = dataSource.getConnection();
            //获取数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //释放资源
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
