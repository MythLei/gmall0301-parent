package com.atguigu.gmall;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Felix
 * @date 2022/8/19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dept {
    private int deptno;
    private String dname;
    private Long ts;
}
