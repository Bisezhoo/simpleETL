package com.bise.simpleETL.common.enums;

import lombok.Data;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/2 19:56
 */
public enum ResponseStatusEnum {
        SUCCESS(0, "success"),
        HBASE_CONFIG_SUCCESS(1, "配置文件已加载"),
        HBASE_CONFIG_EXISTED(-1, "配置文件已存在"),
        HBASE_CONFIG_FAILED(-1, "配置文件加载失败，错误信息为："),
        HBASE_CONFIG_NOFOUND(-1, "没有找到该连接的配置"),
        HBASE_CONNECTION_FAILED_CONFIG_IS_NULL(-1, "获取连接失败，不存在该连接配置"),
        HBASE_CONNECTION_FAILED_IO_ERROR(-1, "获取连接失败，错误信息为："),
        HBASE_TABLE_GET_FAILED_IO_ERROR(-1, "获取Table失败，错误信息为："),
        HBASE_TABLE_CLOSE_FAILED_IO_ERROR(-1, "关闭Table失败，错误信息为："),
        HBASE_TABLE_CREATE_FAILED_IO_ERROR(-1, "创建Table失败，错误信息为："),
        HBASE_TABLE_DROP_FAILED_IO_ERROR(-1, "删除Table失败，错误信息为："),
        HBASE_DATA_PUT_FAILED_IO_ERROR(-1, "数据写入失败，错误信息为："),
        HBASE_DATA_DELETE_FAILED_IO_ERROR(-1, "数据删除失败，错误信息为："),
        HBASE_DATA_SCAN_FAILED_IO_ERROR(-1, "数据读取失败，错误信息为："),
        FAILED(-1, "服务错误");

        // 错误码
        private int code;

        // 错误信息
        private String message;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        ResponseStatusEnum(int code, String message) {
            this.code = code;
            this.message = message;
        }

        ResponseStatusEnum(Exception e) {
            this.code = -1;
            this.message = e.getMessage();
        }

}
