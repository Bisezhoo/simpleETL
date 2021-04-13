package com.bise.simpleETL.hbase.service.impl;

import com.bise.simpleETL.common.ResponseVo;
import com.bise.simpleETL.common.enums.ResponseStatusEnum;
import com.bise.simpleETL.hbase.constants.HBaseConstant;
import com.bise.simpleETL.hbase.entitys.dto.*;
import com.bise.simpleETL.hbase.exceptions.MyHBaseException;
import com.bise.simpleETL.hbase.service.DataService;
import com.bise.simpleETL.hbase.utils.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 23:32
 */
@Service
public class DataServiceImpl implements DataService {

   @Override
   public ResponseVo scanDataByTable(ScanDataDTO scanDataDTO) throws MyHBaseException{
       Table table = HBaseUtil.getTable(scanDataDTO.getConnectionName() ,scanDataDTO.getTableName());
       Scan scan = new Scan();
       try {
           ResultScanner results = table.getScanner(scan);
           List<Map<String, String>> scanResult = new ArrayList<>();
           if(null != scanDataDTO.getLimit()) {
               results.next(scanDataDTO.getLimit());
           }
           for (Result res : results) {
               Map<String, String> map = new HashMap<>();
               String rowKey = Bytes.toString(res.getRow());
               map.put(HBaseConstant.ROW_KEY ,rowKey);
               for (Cell cell : res.rawCells()) {

                   String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                   String dataValue = Bytes.toString(CellUtil.cloneValue(cell));
                   map.put(columnName, dataValue);
               }
               scanResult.add(map);
           }
            return new ResponseVo(ResponseStatusEnum.SUCCESS ,scanResult);
       }catch (IOException e){
           throw new MyHBaseException(ResponseStatusEnum.HBASE_DATA_SCAN_FAILED_IO_ERROR.getMessage() ,e);
       }
   }

   @Override
   public ResponseVo putData2HBase(PutDataDTO putDataDTO) throws MyHBaseException{

       List<Put> puts = new ArrayList<>();

       for (Map<String, String> dataMap : putDataDTO.getDataMap()) {
           String rowKey = dataMap.remove(HBaseConstant.ROW_KEY);
           String columnFamily = dataMap.remove(HBaseConstant.COLUMN_FAMILY);
           Put put = new Put(Bytes.toBytes(rowKey));
           dataMap.forEach((key, value) -> {
               put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(value));
           });
           puts.add(put);
       }

       HBaseUtil.put(putDataDTO.getConnectionName() ,putDataDTO.getTableName() ,puts);
       return new ResponseVo(ResponseStatusEnum.SUCCESS);
    }

    @Override
   public ResponseVo copyData2HBase(CopyDataDTO copyDataDTO) throws MyHBaseException{
       List<CopyTableDTO> tables = copyDataDTO.getTables();

       String uuid = UUID.randomUUID().toString();

       for (CopyTableDTO tableDTO : tables) {
           String sourceTableName = tableDTO.getSourceNameSpace() + ":" + tableDTO.getSourceTableName();
           List<CellDTO> sourceQueryCells = new ArrayList<>();
           String targetTableName = "";

           Map<String ,CellDTO> cellMap = new HashMap<>();

           //如果没有配置目标命名空间，将源命名空间自动导入
           targetTableName += null != tableDTO.getTargetNameSpace() ? tableDTO.getTargetNameSpace() + ":" : tableDTO.getSourceNameSpace() + ":";
           //如果没有配置目标表名，将源表名自动导入
           targetTableName += null != tableDTO.getTargetTableName() ? tableDTO.getTargetTableName() : tableDTO.getSourceTableName();

           if(null != tableDTO.getCells()){
               for (CellDTO[] cellArray : tableDTO.getCells()) {
                   //如果没有配置该列，跳过此列
                   if(null != cellArray && cellArray.length == 0){
                       continue;
                   }
                   CellDTO sourceCellDTO = cellArray[0];
                   CellDTO targetCellDTO = cellArray.length == 2 ? cellArray[1] : new CellDTO();

                   sourceQueryCells.add(sourceCellDTO);

                   String sourceCellName = sourceCellDTO.getColumnFamily() + ":" + sourceCellDTO.getColumnName();
                   targetCellDTO.setColumnFamily(null != targetCellDTO.getColumnFamily() ? targetCellDTO.getColumnFamily() : sourceCellDTO.getColumnFamily());
                   targetCellDTO.setColumnName(null != targetCellDTO.getColumnName() ? targetCellDTO.getColumnName() : sourceCellDTO.getColumnName());
                   cellMap.put(sourceCellName ,targetCellDTO);
               }
           }
           Table table = HBaseUtil.getTable(copyDataDTO.getSourceConnectionName(),sourceTableName );
           //初始化scan
           Scan scan = new Scan();
           scan.setCaching(copyDataDTO.getCacheRowCount());

           for (CellDTO sourceQueryCell : sourceQueryCells) {
               scan.addColumn(Bytes.toBytes(sourceQueryCell.getColumnFamily()) ,Bytes.toBytes(sourceQueryCell.getColumnName()));
           }

           ResultScanner results;

           try {
               results = table.getScanner(scan);
           }catch (IOException e){
               throw new MyHBaseException(ResponseStatusEnum.HBASE_DATA_SCAN_FAILED_IO_ERROR.getMessage() ,e);
           }

//           List<Map<String, String>> scanResult = new ArrayList<>();

           //本次处理的行
           int scanRowCount = 0;

           List<Put> targetPuts = new ArrayList<>();

           for (Result res : results) {
               Map<String, String> rowDataMap = new HashMap<>();

               //获取rowkey
               String rowKey = Bytes.toString(res.getRow());
               rowDataMap.put(HBaseConstant.ROW_KEY, rowKey);

               Put targetPut = new Put(res.getRow());

               //遍历列
               for (Cell cell : res.rawCells()) {
                   String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                   String columnFamilyName = Bytes.toString(CellUtil.cloneFamily(cell));
//                   String dataValue = Bytes.toString(CellUtil.cloneValue(cell));
                   columnName = columnFamilyName + ":" + columnName;

                   byte[] familyBytes = CellUtil.cloneFamily(cell);
                   byte[] columnNameBytes = CellUtil.cloneQualifier(cell);
                   byte[] valueBytes = CellUtil.cloneValue(cell);
                   //处理列映射
                   if(null != tableDTO.getCells()){
                       CellDTO targetCellDTO = cellMap.get(columnName);
                       if(null != targetCellDTO) {
                           familyBytes = Bytes.toBytes(targetCellDTO.getColumnFamily());
                           columnNameBytes = Bytes.toBytes(targetCellDTO.getColumnName());
                       }
                   }

                   targetPut.addColumn(familyBytes ,columnNameBytes ,valueBytes);

//                   rowDataMap.put(columnName, dataValue);
               }
               targetPuts.add(targetPut);
               scanRowCount++;
               if(scanRowCount == copyDataDTO.getCacheRowCount()){
                   HBaseUtil.put(copyDataDTO.getTargetConnectionName() ,targetTableName ,targetPuts);
                   scanRowCount = 0;
                   targetPuts.clear();
               }
           }
           if(scanRowCount > 0){
               HBaseUtil.put(copyDataDTO.getTargetConnectionName() ,targetTableName ,targetPuts ,uuid);
               scanRowCount = 0;
               targetPuts.clear();
           }
       }
       return new ResponseVo(ResponseStatusEnum.SUCCESS ,uuid);
    }
}
