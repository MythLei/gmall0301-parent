package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2022/8/14
 * 对主流和广播流关联后的数据进行处理
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
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
            String sourceTable = after.getSourceTable();

            broadcastState.put(sourceTable,after);
        }

    }
}
