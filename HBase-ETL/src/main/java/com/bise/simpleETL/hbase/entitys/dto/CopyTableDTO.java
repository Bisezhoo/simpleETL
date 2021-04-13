package com.bise.simpleETL.hbase.entitys.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/4 1:05
 */
@Data
public class CopyTableDTO {

    private String sourceNameSpace;

    private String sourceTableName;

    private String targetNameSpace;

    private String targetTableName;

    @ApiModelProperty("列映射,0为源，1为目标")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<CellDTO[]> cells;

}
