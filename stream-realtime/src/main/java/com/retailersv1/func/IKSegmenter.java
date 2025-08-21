package com.retailersv1.func;

import java.io.IOException;
import java.io.Reader;

/**
 * 简化的IK分词器实现
 */
public class IKSegmenter {
    private final Reader input;       // 输入流
    private final boolean useSmart;   // 是否使用智能分词模式
    private int position = 0;         // 当前读取位置
    private char[] buffer = new char[1024];  // 缓冲区
    private int length = 0;           // 缓冲区中有效字符长度

    public IKSegmenter(Reader input, boolean useSmart) {
        this.input = input;
        this.useSmart = useSmart;
    }

    /**
     * 获取下一个分词单元
     * @return 词元对象，没有更多词元时返回null
     * @throws IOException 输入输出异常
     */
    public Lexeme next() throws IOException {
        // 读取数据到缓冲区
        if (position >= length) {
            length = input.read(buffer);
            position = 0;
            if (length == -1) {
                return null;  // 读取完毕
            }
        }

        // 跳过空白字符
        while (position < length) {
            char c = buffer[position];
            if (Character.isWhitespace(c) || isPunctuation(c)) {
                position++;
            } else {
                break;
            }
        }

        if (position >= length) {
            return next();  // 递归处理剩余内容
        }

        // 确定词元结束位置
        int start = position;
        // 智能模式下尝试更长的匹配（简化逻辑）
        if (useSmart) {
            // 这里仅作为示例：连续字母/数字/汉字视为一个词
            char first = buffer[start];
            if (Character.isLetterOrDigit(first) || isChinese(first)) {
                while (position < length) {
                    char current = buffer[position];
                    if (Character.isLetterOrDigit(current) || isChinese(current)) {
                        position++;
                    } else {
                        break;
                    }
                }
            } else {
                position++;  // 非字母数字汉字则单个字符作为词元
            }
        } else {
            // 非智能模式：单个字符作为词元（简化逻辑）
            position++;
        }

        // 创建词元对象
        return new Lexeme(start, position - start, new String(buffer, start, position - start));
    }

    /**
     * 判断是否为汉字
     */
    private boolean isChinese(char c) {
        return c >= 0x4E00 && c <= 0x9FA5;
    }

    /**
     * 判断是否为标点符号
     */
    private boolean isPunctuation(char c) {
        return (c >= 0x21 && c <= 0x2F) ||
                (c >= 0x3A && c <= 0x40) ||
                (c >= 0x5B && c <= 0x60) ||
                (c >= 0x7B && c <= 0x7E) ||
                (c >= 0xFF01 && c <= 0xFF0F) ||
                (c >= 0xFF1A && c <= 0xFF1F) ||
                (c >= 0xFF3B && c <= 0xFF40) ||
                (c >= 0xFF5B && c <= 0xFF60) ||
                (c >= 0xFF61 && c <= 0xFF65);
    }

    /**
     * 分词单元（词元）类
     */
    public static class Lexeme {
        private final int offset;      // 起始偏移量
        private final int length;      // 长度
        private final String lexemeText;  // 词元文本

        public Lexeme(int offset, int length, String lexemeText) {
            this.offset = offset;
            this.length = length;
            this.lexemeText = lexemeText;
        }

        /**
         * 获取词元文本
         */
        public String getLexemeText() {
            return lexemeText;
        }

        public int getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }
    }
}
