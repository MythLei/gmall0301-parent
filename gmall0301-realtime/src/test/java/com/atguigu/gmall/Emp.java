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
public class Emp {
    private int empno;
    private String ename;
    private int deptno;
    private Long ts;
}

