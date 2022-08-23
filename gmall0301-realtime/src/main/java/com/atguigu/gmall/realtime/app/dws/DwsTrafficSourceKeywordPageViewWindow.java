package com.atguigu.gmall.realtime.app.dws;

/**
 * @author Felix
 * @date 2022/8/23
 * 流量域：关键词聚合统计---SQL
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        //1.2 设置并行度
        //1.3 设置表执行环境

        //TODO 2.检查点相关设置(略)

        //TODO 3.从kafka的page_log主题中读取数据  创建动态表  指定Watermark以及提取事件时间字段

        //TODO 4.过滤出搜索行为

        //TODO 5.使用自定义UDTF函数  对搜索内容进行分词 ，并将分词结果和表中的字段进行连接

        //TODO 6.分组、开窗、聚合计算

        //TODO 7.将动态表转换为流

        //TODO 8.将流中的结果写到Clickhouse表中


    }
}
