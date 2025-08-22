package com.retailersv1.func;

import com.retailersv1.stream.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


/**
 * @Package com.retailersv.func
 * @Author xiaoye
 * @Date 2025/8/20 19:29
 * @description:
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF  extends TableFunction<Row> {
    public void eval(String test) {
        for (String keyword : KeywordUtil.analyze(test)) {
            collect(Row.of(keyword));
        }
    }
}