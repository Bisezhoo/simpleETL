package com.bise.simpleETL.hbase.controller;

import com.bise.simpleETL.common.ResponseVo;
import com.bise.simpleETL.common.enums.ResponseStatusEnum;
import com.bise.simpleETL.hbase.entitys.dto.CopyDataDTO;
import com.bise.simpleETL.hbase.entitys.dto.InitHBaseConnectionDTO;
import com.bise.simpleETL.hbase.entitys.dto.PutDataDTO;
import com.bise.simpleETL.hbase.entitys.dto.ScanDataDTO;
import com.bise.simpleETL.hbase.exceptions.MyHBaseException;
import com.bise.simpleETL.hbase.service.DataService;
import com.bise.simpleETL.hbase.utils.HBaseUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 22:58
 */
@RestController
@RequestMapping("/data")
@Api("数据相关")
public class DataController {


    @Autowired
    private DataService dataService;

    @PostMapping(value = "scanDataByTable")
    @ApiOperation(value = "根据表名获取数据", notes = "根据表名获取数据")
    ResponseVo scanDataByTable(@RequestBody ScanDataDTO scanDataDTO) throws MyHBaseException {
        return dataService.scanDataByTable(scanDataDTO);
    }

    @PostMapping(value = "putData2HBase")
    @ApiOperation(value = "put数据到HBase", notes = "put数据到HBase")
    ResponseVo putData2HBase(@RequestBody PutDataDTO putDataDTO) throws MyHBaseException  {
        return dataService.putData2HBase(putDataDTO);
    }

    @PostMapping(value = "copyData2HBase")
    @ApiOperation(value = "copy数据到HBase", notes = "copy数据到HBase")
    ResponseVo copyData2HBase(@RequestBody CopyDataDTO copyDataDTO) throws MyHBaseException {
        return dataService.copyData2HBase(copyDataDTO);
    }

    @PostMapping(value = "getPutErrorMessageById")
    @ApiOperation(value = "根据UID查看错误信息", notes = "根据UID查看错误信息")
    ResponseVo getPutErrorMessageById(String uid) {
        return new ResponseVo(ResponseStatusEnum.SUCCESS ,HBaseUtil.getPutErrorMessageById(uid));
    }


}
