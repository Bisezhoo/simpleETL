package com.bise.simpleETL.hbase.service;

import com.bise.simpleETL.common.ResponseVo;
import com.bise.simpleETL.hbase.entitys.dto.CopyDataDTO;
import com.bise.simpleETL.hbase.entitys.dto.PutDataDTO;
import com.bise.simpleETL.hbase.entitys.dto.ScanDataDTO;
import com.bise.simpleETL.hbase.exceptions.MyHBaseException;
import org.springframework.web.bind.annotation.RequestBody;

import java.io.IOException;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 23:31
 */
public interface DataService {

    ResponseVo scanDataByTable(ScanDataDTO scanDataDTO) throws MyHBaseException;

    ResponseVo putData2HBase(PutDataDTO putDataDTO) throws MyHBaseException;

    ResponseVo copyData2HBase(CopyDataDTO copyDataDTO) throws MyHBaseException;
}
