package com.atguigu.gmall;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Felix
 * @date 2022/8/20
 * 该案例演示了基本的SQL的join方式
 *                              左表                  右表
 *          内连接          OnCreateAndWrite       OnCreateAndWrite
 *          左外连接        OnReadAndWrite         OnCreateAndWrite
 *          右外连接        OnCreateAndWrite       OnReadAndWrite
 *          全外连接        OnReadAndWrite         OnReadAndWrite
 */
public class TestSqlJoin {
    public static void main(String[] args) {
        //TODO 1.基本环境的准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.4 设置join时候 状态的失效时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Emp> empDS = env
            .socketTextStream("hadoop202", 8888)
            .map(new MapFunction<String, Emp>() {
                @Override
                public Emp map(String str) throws Exception {
                    String[] fieldArr = str.split(",");
                    return new Emp(
                        Integer.parseInt(fieldArr[0]),
                        fieldArr[1],
                        Integer.parseInt(fieldArr[2]),
                        Long.valueOf(fieldArr[3])
                    );
                }
            });


        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop202", 8889).map(
            lineStr -> {
                String[] fieldArr = lineStr.split(",");
                return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
            }
        );

        //TODO 2.将流转换为表
        tableEnv.createTemporaryView("emp",empDS);
        tableEnv.createTemporaryView("dept",deptDS);

        //TODO 3.内连接
        //注意：FlinkSQL在进行基本内外连接的时候，底层会维护两个状态，用于存放左表和右表的数据
        //默认情况下，状态中的数据永不失效
        // tableEnv.executeSql("select * from emp e,dept d where e.deptno = d.deptno").print();

        //TODO 4.左外连接
        //左外连接关联过程
        //如果左表数据先到，右表数据后到，会先生成一条数据 [左表 + null]，标记为+I
        //当右表数据到的时候，会再生成一条数据       [左表 + null]，标记为-D
        //最后生成一条数据  [左表 + 右表]，标记为+I
        //这种动态表转换的流称之为回撤流
        // tableEnv.executeSql("select * from emp e left join dept d on e.deptno = d.deptno").print();

        //TODO 5.右外连接
        // tableEnv.executeSql("select * from emp e right join dept d on e.deptno = d.deptno").print();

        //TODO 6.全外连接
        // tableEnv.executeSql("select * from emp e full join dept d on e.deptno = d.deptno").print();

        //TODO 7.将左外连接之后的数据  写到kafka的主题中
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
            "  empno INTEGER,\n" +
            "  ename string,\n" +
            "  deptno integer,\n" +
            "  dname string,\n" +
            "  PRIMARY KEY (empno) NOT ENFORCED \n" +
            ") WITH (\n" +
            "  'connector' = 'upsert-kafka',\n" +
            "  'topic' = 'first',\n" +
            "  'properties.bootstrap.servers' = 'hadoop202:9092',\n" +
            "  'key.format' = 'json',\n" +
            "  'value.format' = 'json'\n" +
            ")");

        tableEnv
            .executeSql("insert into emp_dept select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno = d.deptno");


    }
}
