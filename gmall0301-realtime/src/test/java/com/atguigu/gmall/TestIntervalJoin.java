package com.atguigu.gmall;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2022/8/19
 * 该案例演示了IntervalJoin的使用
 */
public class TestIntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

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
            }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Emp>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Emp>() {
                            @Override
                            public long extractTimestamp(Emp emp, long recordTimestamp) {
                                return emp.getTs();
                            }
                        }
                    )
            );

        empDS.print("emp:");
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop202", 8889).map(
            lineStr -> {
                String[] fieldArr = lineStr.split(",");
                return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
            }
        ).assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<Dept>forMonotonousTimestamps()
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<Dept>() {
                        @Override
                        public long extractTimestamp(Dept dept, long recordTimestamp) {
                            return dept.getTs();
                        }
                    }
                )
        );
        deptDS.print("dept:");

        //使用intervalJoin对两条流进行连接
        SingleOutputStreamOperator<Tuple2<Emp, Dept>> processDS = empDS
            .keyBy(Emp::getDeptno)
            .intervalJoin(deptDS.keyBy(Dept::getDeptno))
            .between(Time.milliseconds(-5), Time.milliseconds(5))
            .process(
                new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                    @Override
                    public void processElement(Emp left, Dept right, Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                        out.collect(Tuple2.of(left, right));
                    }
                }
            );

        processDS.print(">>>");

        env.execute();
    }
}
