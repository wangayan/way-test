package com.way.day0826;

/**
 * @Title: Test02
 * @Author wang.Ayan
 * @Date 2025/8/26 21:29
 * @Package com.way.day0826
 * @description: leecode第一题，求两数之和
 */
public class Test02 {
    public int[] twoSum ( int[] a , int b){
        for (int i = 0; i < a.length; i++) {
            for (int j = i + 1; j < a.length; j++) {
                if (a[i] + a[j] == b) {
                    int[] result={i,j};
                    return result;
                }
            }
        }
        return null; // 未找到结果
    }
}

