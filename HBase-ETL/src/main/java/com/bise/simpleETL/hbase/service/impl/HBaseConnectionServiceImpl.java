package com.bise.simpleETL.hbase.service.impl;

import com.bise.simpleETL.hbase.service.HBaseConnectionService;
import com.bise.simpleETL.hbase.utils.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 20:06
 */
@Slf4j
@Component
public class HBaseConnectionServiceImpl implements HBaseConnectionService {

    @Override
    @PreDestroy
    public void closeAllConnection(){
        HBaseUtil.closeAllConnection();
        log.warn("正在清除所有HBase连接");
    }

}
