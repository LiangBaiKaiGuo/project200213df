package com.atguigu;

import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Guo
 * @create 2020-06-22-11:03
 */
public class Hbase_DML {

    private static Connection connection;


    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //创建连接器
            connection = ConnectionFactory.createConnection(configuration);
            //创建DDL对象
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //释放资源
    public static void closeConnection() throws IOException {
        connection.close();
    }

    //todo 插入数据
    public static void putData(String tableName,String rowkey,String cf,String cn,String value) throws IOException {
        //1.获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.获取put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        //3.插入数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //4.执行插入操作
        table.put(put);
        //5.关闭表连接
        table.close();
    }

    //获取单条数据get
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

//        get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //执行查询操作
        Result result = table.get(get);
        //解析result
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell))+
                    " CN:" +  Bytes.toString(CellUtil.cloneQualifier(cell))+
                    " value:" + Bytes.toString(CellUtil.cloneValue(cell)));

        }
    }

    //todo scan 扫描数据
    public static void scanData(String tableName) throws IOException {
        //获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建Scan对象
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("1001"),false);
        scan.withStopRow(Bytes.toBytes("1004"),true);
        //扫描全表
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            //解析result
            for (Cell cell : result.rawCells()) {
                System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell))+
                        " CN:" +  Bytes.toString(CellUtil.cloneQualifier(cell))+
                        " value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }
    }

    //todo 删除数据
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //指定删除列族和列名的数据
//        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
//        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //指定删除列族数据
//        delete.addFamily(Bytes.toBytes(cf));

        //执行删除操作
        table.delete(delete);

    }


    public static void main(String[] args) throws IOException {
//        putData("stu2","1001","info","name","zhangsan");
//        getData("stu2","1001","info","age");
//        scanData("stu2");
        deleteData("stu2","1002","info","name");
        closeConnection();

    }
}
