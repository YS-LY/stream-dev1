package com.retailersv1.func;



import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IkUtil {

    // 修改为List保留重复词，如需去重可改为HashSet
    public static List<String> split(String s) {
        List<String> result = new ArrayList<>();
        if (s == null || s.trim().isEmpty()) {
            return result;
        }

        Reader reader = new StringReader(s);
        // 智能分词模式
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            IKSegmenter.Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);  // 保留重复词
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            throw new RuntimeException("IK分词失败", e);
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                // 忽略关闭异常
            }
        }
        return result;
    }
}
