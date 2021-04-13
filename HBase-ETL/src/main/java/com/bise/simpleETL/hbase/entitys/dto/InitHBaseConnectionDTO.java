package com.bise.simpleETL.hbase.entitys.dto;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 19:14
 */
@Data
public class InitHBaseConnectionDTO {


    @ApiModelProperty("连接名/ID")
    @NotBlank(message = "连接名不能为空")
    private String connectionName;

    @ApiModelProperty("连接地址")
    @NotBlank(message = "连接地址不能为空")
    private String zkHost;

    @ApiModelProperty(name = "zk节点名称" ,value = "zk节点名称(一般是/hbase)")
    @NotBlank(message = "zk节点名称不能为空")
    private String nodeParent;

    @ApiModelProperty(name = "最大线程数" ,value = "最大线程数(默认10)" ,example = "10")
    @NotBlank(message = "最大线程数不能为空")
    @Pattern(regexp = "^[0-9]*$ " ,message = "最大线程数只能是数字")
    private String threadsMax = "10";

    @ApiModelProperty(name = "核心线程数" ,value = "核心线程数(默认4)" ,example = "4")
    @NotBlank(message = "核心线程数不能为空")
    @Pattern(regexp = "^[0-9]*$ " ,message = "核心线程数只能是数字")
    private String threadsCoreSize = "4";

}
