package com.bise.simpleETL.hbase.entitys.dto;

import lombok.Data;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 23:05
 */
@Data
public class ScanDataDTO {


    private String connectionName;

    private String tableName;

    private Integer limit;

}
