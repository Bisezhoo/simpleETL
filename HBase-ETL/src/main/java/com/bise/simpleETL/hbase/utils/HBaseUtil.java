package com.bise.simpleETL.hbase.utils;

import com.bise.simpleETL.common.ResponseVo;
import com.bise.simpleETL.common.enums.ResponseStatusEnum;
import com.bise.simpleETL.hbase.exceptions.MyHBaseException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HBase 工具类
 * Created by babylon on 2016/11/29.
 */
@Slf4j
@Data
public class HBaseUtil {


    private static Map<String ,Configuration> configurationMap = new ConcurrentHashMap<>();
    private static Map<String ,Connection> connectionMap = new ConcurrentHashMap<>();
    private static Map<String ,Exception> errorMap = new ConcurrentHashMap<>();

    public static String init(String connectionName, String zkHost, String pt ,String threadsMax ,String threadsCoreSize) {
        try {
            Configuration conf = configurationMap.get(connectionName);
            if (conf == null) {
                conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", zkHost);
                conf.set("zookeeper.znode.parent", pt);
                conf.setInt("hbase.htable.threads.max", Integer.valueOf(threadsMax));
                conf.setInt("hbase.client.ipc.pool.size", Integer.valueOf(threadsMax));
                conf.setInt("hbase.htable.threads.coresize", Integer.valueOf(threadsCoreSize));
                conf.setInt("hbase.hconnection.threads.max", Integer.valueOf(threadsMax));
                conf.setInt("hbase.hconnection.threads.core", Integer.valueOf(threadsCoreSize));
                conf.setLong("hbase.hconnection.threads.keepalivetime", 300);
//                conf.set("hbase.rpc.timeout", "1800000");
//                conf.set("hbase.client.scanner.timeout.period", "1800000");
                configurationMap.put(connectionName ,conf);
                return ResponseStatusEnum.HBASE_CONFIG_SUCCESS.getMessage();
            }else {
                return ResponseStatusEnum.HBASE_CONFIG_EXISTED.getMessage();
            }
        } catch (Exception e) {
            log.error("HBase Configuration Initialization failure !");
            e.printStackTrace();
            return ResponseStatusEnum.HBASE_CONFIG_FAILED.getMessage() + e.getMessage();
        }
    }

    /**
     * 查看所有连接
     *
     * @return
     */
    public static List<String> queryAllConnection(){
        return new ArrayList<String>(configurationMap.keySet());
    }

    /**
     * 根据连接名获取连接配置
     *
     * @return
     */
    public static ResponseVo queryConnectionConfigByName(String connectionName){
        Configuration configuration = configurationMap.get(connectionName);
        if(null == configuration){
            return new ResponseVo(ResponseStatusEnum.HBASE_CONFIG_NOFOUND);
        }
        return new ResponseVo(ResponseStatusEnum.SUCCESS ,configuration.iterator());
    }
    /**
     * 关闭所有活动的HBase连接
     *
     * @return
     */
    public static void closeAllConnection(){
        connectionMap.entrySet().forEach(connectionEntry -> {
            closeConnect(connectionEntry.getKey());
        });
    }

    /**
     * 获取单条数据
     *
     * @param tablename
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static Result getRow(String connectionName, String tablename, String rowKey) throws MyHBaseException {
        Table table = getTable(connectionName ,tablename);
        Result rs = null;
        if (table != null) {
            try {
                Get g = new Get(Bytes.toBytes(rowKey));
                rs = table.get(g);
            } catch (IOException e) {
                throw new MyHBaseException(ResponseStatusEnum.HBASE_TABLE_GET_FAILED_IO_ERROR.getMessage() ,e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    throw new MyHBaseException(ResponseStatusEnum.HBASE_TABLE_CLOSE_FAILED_IO_ERROR.getMessage() ,e);
                }
            }
        }
        return rs;
    }

    /**
     * 获得链接
     *
     * @return
     */
    public static synchronized Connection getConnection(String connectionName) throws MyHBaseException {
            Connection conn = connectionMap.get(connectionName);
            if (conn == null || conn.isClosed()) {
                Configuration conf = configurationMap.get(connectionName);
                if(null == conf){
                    throw new MyHBaseException(ResponseStatusEnum.HBASE_CONNECTION_FAILED_CONFIG_IS_NULL.getMessage());
                }
                try {
                    conn = ConnectionFactory.createConnection(conf);
                }catch (IOException e){
                    throw new MyHBaseException(ResponseStatusEnum.HBASE_CONNECTION_FAILED_IO_ERROR.getMessage() ,e);
                }
                connectionMap.put(connectionName ,conn);
            }
            log.warn("HBase连接:" + conn.getConfiguration().get("hbase.zookeeper.quorum"));
            return conn;
    }

    public static ResponseVo createTable(String connectionName , String table , String familyName ,String namespace ,Boolean isCompression) throws MyHBaseException {
        Connection conn = getConnection(connectionName);
        try {
            Admin admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(table);//表的名称
            HTableDescriptor desc = new HTableDescriptor(tableName);

            //创建列簇的描述类
            HColumnDescriptor cf = new HColumnDescriptor(familyName);
            if (isCompression) {
                desc.setCompactionEnabled(isCompression);
                cf.setCompressionType(Compression.Algorithm.SNAPPY);
            }
            //将列簇添加到表中
            desc.addFamily(cf);
            admin.createTable(desc);//创建表
        }catch (IOException e){
            throw new MyHBaseException(ResponseStatusEnum.HBASE_TABLE_CREATE_FAILED_IO_ERROR.getMessage() ,e);
        }
        return new ResponseVo(ResponseStatusEnum.SUCCESS);
/*
        TableDescriptorBuilder descb = TableDescriptorBuilder.newBuilder(tableName);
        descb.setCompactionEnabled(true);
        //创建列簇的描述类
        ColumnFamilyDescriptorBuilder cfb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
        cfb.setCompressionType(Compression.Algorithm.SNAPPY);
        //将列簇添加到表中
        descb.setColumnFamily(cfb.build());
        TableDescriptor tdesc = descb.build();
        admin.createTable(tdesc);//创建表*/
    }
    public static void createTable(String connectionName ,String table ,String familyName ,Boolean isCompression) throws MyHBaseException {
        createTable(connectionName ,table ,familyName ,null ,isCompression);
    }

    /**
     * 删除一张表
     * @param tableName
     */
    public static void dropTable(String connectionName ,String tableName) throws MyHBaseException {
        Connection conn = getConnection(connectionName);
        try {
            Admin admin = conn.getAdmin();
            // 禁用
            admin.disableTable(TableName.valueOf(tableName));
            // 删除
            admin.deleteTable(TableName.valueOf(tableName));
        }catch (IOException e){
            throw new MyHBaseException(ResponseStatusEnum.HBASE_TABLE_DROP_FAILED_IO_ERROR.getMessage() ,e);
        }
    }


    /**
     * 获取  Table
     *
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static Table getTable(String connectionName ,String tableName) throws MyHBaseException{
        try {
            return getConnection(connectionName).getTable(TableName.valueOf(tableName));
        }catch (IOException e){
            throw new MyHBaseException(ResponseStatusEnum.HBASE_TABLE_GET_FAILED_IO_ERROR.getMessage() ,e);
        }
    }


    /**
     * 异步往指定表添加数据
     *
     * @param tablename 表名
     * @param puts      需要添加的数据
     * @return long      返回执行时间
     * @throws IOException
     */
    public static long put(String connectionName ,String tablename, List<Put> puts) throws MyHBaseException {
        return put(connectionName ,tablename ,puts ,null);
    }

    public static Exception getPutErrorMessageById(String uid){
        return errorMap.get(uid);
    }

       public static long put(String connectionName ,String tablename, List<Put> puts ,String uuid) throws MyHBaseException {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection(connectionName);
        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                e.printStackTrace();
                if(null != uuid){
                    errorMap.put(uuid ,e);
                }
                log.error("Failed to sent put " + e.getRow(i) + ".");
                log.error("{}", e);
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                .listener(listener);
        params.writeBufferSize(30 * 1024 * 1024);

        BufferedMutator mutator = null;

        try {
            mutator = conn.getBufferedMutator(params);
            mutator.mutate(puts);
            mutator.flush();
        }catch (Exception e){
            throw new MyHBaseException(ResponseStatusEnum.HBASE_DATA_PUT_FAILED_IO_ERROR.getMessage() ,e);
        }
        finally {
            if(null != mutator) {
                try {
                    mutator.close();
                }catch (IOException e){
                    throw new MyHBaseException(ResponseStatusEnum.HBASE_DATA_PUT_FAILED_IO_ERROR.getMessage() ,e);
                }
            }
//            closeConnect(conn);
        }
        return System.currentTimeMillis() - currentTime;
    }

    // 根据rowKey前缀及字段名删除表数据
    public static void deleteDataByRowKeyAndCloumn(String connectionName ,String tableName, String typeKey, String dataKey, String columnFamily) throws MyHBaseException {
        Table table = HBaseUtil.getTable(connectionName ,tableName);
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(typeKey.getBytes()));
        if (StringUtils.isNotBlank(dataKey)) {
            scan.addColumn(columnFamily.getBytes(), dataKey.getBytes());
        }
        ResultScanner results = null;
        try {
            results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        List<Delete> list = new ArrayList();
        while (it.hasNext()) {
            Result delete = it.next();
            Delete d = new Delete(delete.getRow());
            if (StringUtils.isNotBlank(dataKey)) {
                d.addColumn(columnFamily.getBytes(), dataKey.getBytes());
            }
            list.add(d);
        }
        table.delete(list);
        } catch (IOException e) {
            throw new MyHBaseException(ResponseStatusEnum.HBASE_DATA_DELETE_FAILED_IO_ERROR.getMessage() ,e);
        }
    }


    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnect(String connectionName) {
        Connection conn = connectionMap.get(connectionName);
        if (null != conn) {
            try {
                conn.close();
                connectionMap.remove(connectionName);
                configurationMap.remove(connectionName);
                log.error("关闭连接:" + connectionName);
            } catch (Exception e) {
                log.error("closeConnect failure !", e);
            }
        }
    }

    /**
     * 根据family和列名从Result里取数据
     *
     * @param
     * @return
     */
    public static String getColumnValue(Result result, String family, String columnName) {
        String columnValue = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(columnName)));
        if (StringUtils.isBlank(columnValue) || "null".equals(columnValue) || "NULL".equals(columnValue) || "Null".equals(columnValue)) {
            return null;
        }
        return columnValue;
    }

}
