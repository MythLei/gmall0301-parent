package com.atguigu.gmall;

import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2022/8/17
 */
public class TestOutputTag {
    public static void main(String[] args) {
        // OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag");
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
    }
}
