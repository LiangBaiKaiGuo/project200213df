package com.Exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Guo
 * @create 2020-06-22-18:42
 */
public class Exer_DML02 {
    private static Connection connection;


    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //创建连接器
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        connection.close();
    }

    //todo 插入数据单个数据
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        //获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //执行put操作
        table.put(put);

        //释放资源
        table.close();
    }

    //todo 获取单个数据get
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //执行get操作
        Result result = table.get(get);
        //解析result
        for (Cell cell : result.rawCells()) {
            System.out.println(" CF: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    " CN: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    " value: " + Bytes.toString(CellUtil.cloneValue(cell)) );
        }
        table.close();
    }

    //todo 扫描数据 scan
    public static void scanTable(String tableName) throws IOException {
        //获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建scan对象
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("1001"),false);
        scan.withStopRow(Bytes.toBytes("1004"),false);
        //执行scan操作
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            //解析result
            for (Cell cell : result.rawCells()) {
                System.out.println(" CF: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        " CN: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        " value: " + Bytes.toString(CellUtil.cloneValue(cell)) );
            }
        }

        table.close();
    }

    //todo 删除数据
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //执行delete
        table.delete(delete);



        table.close();

    }

    public static void main(String[] args) throws IOException {
//        putData("exer","1002","info2","name","x1");
//        getData("exer","1001","info1","name");
//        scanTable("exer");
        deleteData("exer","1002","info2","name");
        close();
    }
}
