package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Felix
 * @date 2022/8/26
 * Desc: 流量域首页、详情页访问实体类
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    Long ts;
}

