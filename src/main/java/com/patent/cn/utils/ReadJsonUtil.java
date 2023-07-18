package com.patent.cn.utils;

import cn.hutool.core.io.IoUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author ：hanyc
 * @date ：2023/4/18 10:52
 * @description： 解析 json文件
 */
@Slf4j
public class ReadJsonUtil {

    /**
     * 读取JSON 文件内容
     *
     * @param inputStream 文件流
     * @return
     */
    public static List<String> resultList(InputStream inputStream) {
        List<String> list = new LinkedList<>();
        BufferedReader reader = null;
        String line;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
            reader.close();
        } catch (Exception e) {
            log.error("读取JSON文件错误:{}", e.getMessage(), e);
        } finally {
            IoUtil.close(reader);
        }
        return list;
    }

}
