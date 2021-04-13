package com.bise.simpleETL.hbase.controller;

import com.bise.simpleETL.common.ResponseVo;
import com.bise.simpleETL.common.enums.ResponseStatusEnum;
import com.bise.simpleETL.hbase.entitys.dto.InitHBaseConnectionDTO;
import com.bise.simpleETL.hbase.service.HBaseConnectionService;
import com.bise.simpleETL.hbase.utils.HBaseUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.AssertTrue;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/2 19:42
 */
@Validated
@RestController
@RequestMapping("/connection")
@Api("连接相关")
public class ConnectionController {

    @Autowired
    private HBaseConnectionService HBaseConnectionService;

    @PostMapping(value = "initHBaseConnection")
    @ApiOperation(value = "初始化连接", notes = "初始化连接")
    ResponseVo initHBaseConnection(@RequestBody InitHBaseConnectionDTO initDTO) {
        return new ResponseVo(ResponseStatusEnum.SUCCESS,HBaseUtil.init(initDTO.getConnectionName() ,initDTO.getZkHost() ,initDTO.getNodeParent() ,initDTO.getThreadsMax() ,initDTO.getThreadsCoreSize()));
    }

    @PostMapping(value = "closeHBaseConnection")
    @ApiOperation(value = "关闭连接", notes = "关闭连接")
    ResponseVo closeHBaseConnection(String connectionName) {
        HBaseUtil.closeConnect(connectionName );
        return new ResponseVo(ResponseStatusEnum.SUCCESS);
    }

    @GetMapping(value = "queryAllConnection")
    @ApiOperation(value = "查看所有连接", notes = "查看所有连接")
    ResponseVo queryAllConnection() {
        return new ResponseVo(ResponseStatusEnum.SUCCESS,HBaseUtil.queryAllConnection());
    }

    @PostMapping(value = "queryConnectionConfigByName")
    @ApiOperation(value = "根据连接名获取连接配置", notes = "根据连接名获取连接配置")
    ResponseVo queryConnectionConfigByName(String connectionName) {
        return new ResponseVo(ResponseStatusEnum.SUCCESS,HBaseUtil.queryConnectionConfigByName(connectionName));
    }

    @GetMapping(value = "closeAllConnection")
    @ApiOperation(value = "关闭所有连接", notes = "关闭所有连接,防误操作，需要输入true")
    ResponseVo closeAllConnection(@ApiParam("防误操作，需要输入true")@AssertTrue(message = "防误操作，需要输入true") @RequestParam(required = true ,defaultValue = "false") Boolean pass) {
        HBaseConnectionService.closeAllConnection();
        return new ResponseVo(ResponseStatusEnum.SUCCESS);
    }

}
