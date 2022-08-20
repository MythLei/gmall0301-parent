package com.atguigu.gmall;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2022/8/20
 * 该案例演示了lookupJoin
 */
public class TestLookupJoin {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关设置(略)
        //TODO 3.从kafka主题中读取emp数据(事实表)
        tableEnv.executeSql("CREATE TABLE emp (\n" +
            "  empno integer,\n" +
            "  ename STRING,\n" +
            "  deptno integer,\n" +
            "  proc_time AS PROCTIME()\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'first',\n" +
            "  'properties.bootstrap.servers' = 'hadoop202:9092',\n" +
            "  'properties.group.id' = 'testGroup',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'format' = 'json'\n" +
            ")");

        // tableEnv.executeSql("select * from emp").print();

        //TODO 4.从mysql表中读取dept数据(字典维度表)
        tableEnv.executeSql("CREATE TABLE dept (\n" +
            "  deptno integer,\n" +
            "  dname STRING,\n" +
            "  PRIMARY KEY (deptno) NOT ENFORCED\n" +
            ") WITH (\n" +
            "   'connector' = 'jdbc',\n" +
            "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
            "   'url' = 'jdbc:mysql://hadoop202:3306/gmall0301_config',\n" +
            "   'table-name' = 't_dept',\n" +
            "   'lookup.cache.max-rows' = '200',\n" +
            "   'lookup.cache.ttl' = '1 hour',\n" +
            "   'username' = 'root',\n" +
            "   'password' = '123456'\n" +
            ")");
        // tableEnv.executeSql("select * from dept").print();

        //TODO 5.将员工和部门进行连接(lookupJoin)
        //注意：如果是普通的内外连接，不管是左表数据过来，还是右表数据，都会尝试和对方进行连接
        //如果是lookupjoin的话，它是以左表进行驱动，当左表数据过来的时候，才会到右表中查询与之关联的数据
        //所以针对lookupjoin来讲，底层并没有维度什么状态，用于存放两张表数据
        //可以通过连接相关的属性，对右表数据进行缓存
        /*tableEnv.executeSql("SELECT e.empno,e.ename,d.deptno,d.dname FROM emp AS e " +
            " JOIN dept FOR SYSTEM_TIME AS OF e.proc_time AS d ON e.deptno = d.deptno").print();*/
        Table resTable = tableEnv.sqlQuery("SELECT e.empno,e.ename,d.deptno,d.dname FROM emp AS e " +
            " JOIN dept FOR SYSTEM_TIME AS OF e.proc_time AS d ON e.deptno = d.deptno");

        //TODO 6.将连接的结果写到kafka主题中
        //6.1 创建动态表 和要写入的kafka主题进行映射
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
            "  empno integer,\n" +
            "  ename STRING,\n" +
            "  deptno integer,\n" +
            "  dname string,\n" +
            "  PRIMARY KEY (empno) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'upsert-kafka',\n" +
            "  'topic' = 'second',\n" +
            "  'properties.bootstrap.servers' = 'hadoop202:9092',\n" +
            "  'key.format' = 'json',\n" +
            "  'value.format' = 'json'\n" +
            ")");
        tableEnv.executeSql("insert into emp_dept select * from " + resTable);
    }
}
