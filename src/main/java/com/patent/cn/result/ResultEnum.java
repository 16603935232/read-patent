package com.patent.cn.result;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author haoxiaoming
 */
@Getter
@AllArgsConstructor
public enum ResultEnum {

    // 成功
    OK(0, "ok"),
    // 失败
    FAIL(-1, "fail"),
    // 余额不足
    INSUFFICIENT_FUNDS(-2, "积分余额不足"),
    // 文件导入失败
    FILE_FAIL(-3, "文件导入失败");

    private final Integer code;
    private final String msg;

}
