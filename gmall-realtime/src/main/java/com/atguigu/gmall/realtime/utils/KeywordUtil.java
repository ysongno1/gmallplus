package com.atguigu.gmall.realtime.utils;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: IK分词器工具类
 */
public class KeywordUtil {

    public static List<String> splitKeyWord(String keyWord) throws IOException {

        //创建集合用于存放最终的数据结果
        ArrayList<String> resultList = new ArrayList<>();

        //创建IK分词器
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyWord), false);

        //获取切分出来的单词
        Lexeme next = ikSegmenter.next();

        while (next != null) {
            String line = next.getLexemeText();
            resultList.add(line);
            next = ikSegmenter.next();
        }


        return resultList;

    }
}
