package com.Exer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Guo
 * @create 2020-06-22-18:41
 */
public class Exer_DDL01 {
    private static Connection connection;
    private static Admin admin;

    static {
        try {
            //获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //获取连接器
            connection = ConnectionFactory.createConnection(configuration);
            //创建DDL对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        connection.close();
        admin.close();
    }
    //todo 创建命名空间
    public static void createSN(String sn) throws IOException {

        NamespaceDescriptor build = NamespaceDescriptor.create(sn).build();
        admin.createNamespace(build);
//        admin.deleteNamespace(sn);//删除命名空间
    }

    //todo 判断表是否存在
    public static boolean isExitsTable(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    //todo 创建表
    public static void createTB(String tableName,String ... cfs) throws IOException {
        //判断可变形参是否为0， 创建表必须传列族信息
        if (cfs.length<=0){
            System.out.println("请输入列族信息！！！");
            return;
        }
        //判断表是否存在
        if (isExitsTable(tableName)){
            System.out.println("表：" + tableName + " 已经存在！！！");
            return;
        }
        //创建表描述器
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String cf : cfs) {
            ColumnFamilyDescriptor build =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            tableDescriptorBuilder.setColumnFamily(build);
        }
        TableDescriptor descriptor = tableDescriptorBuilder.build();

        //创建表
        admin.createTable(descriptor);
    }

    //todo 删除表
    public static void deleteTB(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        //使表不可用
        admin.disableTable(name);
        //删除表
        admin.deleteTable(name);

    }


    public static void main(String[] args) throws IOException {
//        createSN("bigtable");
        createTB("exer","info1","info2");
//        deleteTB("bigtable:exer");

        close();
    }
}
