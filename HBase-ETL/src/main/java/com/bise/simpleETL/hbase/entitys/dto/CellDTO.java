package com.bise.simpleETL.hbase.entitys.dto;

import lombok.Data;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/4 0:53
 */
@Data
public class CellDTO {

    private String nameSpace;

    private String tableName;

    private String columnFamily;

    private String columnName;


}
