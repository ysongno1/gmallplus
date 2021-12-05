package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

//输出数据的类型和个数
@FunctionHint(output = @DataTypeHint("ROW<word STRING>")) //word是函数默认的列名
public class splitFunction extends TableFunction<Row> {

    public void eval(String str) {
        List<String> splitKeyWord = null;
        try {
            splitKeyWord = KeywordUtil.splitKeyWord(str);
            for (String word : splitKeyWord) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }

    }
}
