package com.retailersv1.stream.utils;


import com.retailersv1.func.IKSegmenter;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.stream.utils
 * @Author xiaoye
 * @Date 2025/8/20 19:06
 * @description: ik分词器
 */
public class KeywordUtil {
    public static List<String> analyze(String text){
        StringReader reader = new StringReader(text);
        List<String> keywordList = new ArrayList<>();
        IKSegmenter ik = new IKSegmenter(reader, true);
        try {
            IKSegmenter.Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null) {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return keywordList;
    }
}
