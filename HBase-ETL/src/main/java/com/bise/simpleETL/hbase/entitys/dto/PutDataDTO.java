package com.bise.simpleETL.hbase.entitys.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 23:31
 */
@Data
public class PutDataDTO {

    private String connectionName;

    private String tableName;

    private List<Map<String ,String>> dataMap;

}
