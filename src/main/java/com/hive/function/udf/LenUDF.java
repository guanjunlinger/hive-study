package com.hive.function.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *  支持简单类型
 */
@Description(name = "LenUDF", value = "compute string len")
public class LenUDF extends UDF {

    public Integer evaluate(String src) {
        return src.length();
    }

}
