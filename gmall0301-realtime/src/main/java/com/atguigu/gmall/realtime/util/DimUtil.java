package com.atguigu.gmall.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Felix
 * @date 2022/8/27
 * 查询维度的工具类
 */
public class DimUtil {
    //从phoenix表中查询维度数据  没有任何优化
    public static JSONObject getDimInfoNoCache(Connection conn, String tableName, Tuple2<String,String> ... columnNameAndValues){

        //拼接查询维度的SQL
        StringBuilder selectSql = new StringBuilder("select * from "+ GmallConfig.PHOENIX_SCHEMA +"."+tableName+" where ");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + "='"+columnValue+"'");
            if(i < columnNameAndValues.length - 1){
                selectSql.append(" and ");
            }
        }
        System.out.println("到phoenix表中查询维度数据的SQL:" + selectSql);
        //底层还是通过phoenixUtil工具类中的方法 到phoenix表中查询数据
        List<JSONObject> jsonObjList = PhoenixUtil.queryList(conn, selectSql.toString(), JSONObject.class);
        JSONObject dimJsonObj = null;
        if(jsonObjList != null && jsonObjList.size() > 0){
            dimJsonObj = jsonObjList.get(0);
        }else{
            System.out.println("在phoenix表中没有找到对应的维度数据");
        }
        return dimJsonObj;
    }

    public static void main(String[] args) throws SQLException {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        Connection conn = dataSource.getConnection();
        System.out.println(getDimInfoNoCache(conn, "dim_base_trademark",Tuple2.of("id","1")));
    }
}
