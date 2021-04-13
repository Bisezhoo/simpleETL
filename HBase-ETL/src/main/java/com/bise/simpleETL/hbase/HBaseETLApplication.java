package com.bise.simpleETL.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * @Author huangxiong
 * @Date 2019/8/20
 **/
@ComponentScan({"com.bise.simpleETL.*"})
@SpringBootApplication
public class HBaseETLApplication {
    public static void main(String[] args) {
        SpringApplication.run(HBaseETLApplication.class, args);
    }
}
