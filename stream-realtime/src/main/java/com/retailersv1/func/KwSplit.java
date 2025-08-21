package com.retailersv1.func;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("row<keyword string>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String kw) {
        if (kw == null || kw.trim().isEmpty()) {
            return;
        }
        // 调用修改后的分词工具类（返回List）
        List<String> keywords = IkUtil.split(kw);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }
}

