package com.bise.simpleETL.hbase.entitys.dto;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/4 0:47
 */
@Data
public class CopyDataDTO {

    @ApiModelProperty("源数据连接")
    private String sourceConnectionName;

    @ApiModelProperty("目标数据连接")
    private String targetConnectionName;

    @ApiModelProperty("表")
    private List<CopyTableDTO> tables;

    @ApiModelProperty("缓存行的数量，可以减少RPC次数增强性能")
    private int cacheRowCount = 10;


}
