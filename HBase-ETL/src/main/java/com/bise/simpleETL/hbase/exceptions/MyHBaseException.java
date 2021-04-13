package com.bise.simpleETL.hbase.exceptions;

/**
 * Created with IDEA
 *
 * @author:Bise
 * @date:2021/4/3 17:34
 */
public class MyHBaseException extends Exception {

    public MyHBaseException() {
        super();
    }

    //定义有参构造方法
    public MyHBaseException(String message) {
        super(message);
    }
    //定义有参构造方法
    public MyHBaseException(String message ,Exception e) {
        super(message + e.getMessage());
        super.setStackTrace(e.getStackTrace());
    }

}
