package com.bise.simpleETL.hbase.service;

import org.springframework.stereotype.Service;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 20:06
 */
public interface HBaseConnectionService {
    void closeAllConnection();
}
