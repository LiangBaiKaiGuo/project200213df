package com.atguigu;


import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Guo
 * @create 2020-06-22-11:03
 */
public class Hbase_DDL {

    private static Connection connection;
    private static Admin admin;


    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //创建连接器
            connection = ConnectionFactory.createConnection(configuration);
            //创建DDL对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //创建命名空间
    public static void createNamespace(String nameSpace) throws IOException {

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);

    }


    //判断表是否存在
    public static boolean isExists(String tableName) throws IOException {
        return  admin.tableExists(TableName.valueOf(tableName));
    }
    //创建表
    public static void createTable(String tableName,String...cfn) throws IOException {
        //判断是否输入列族信息
        if (cfn.length <= 0){
            System.out.println("请输入列族信息！！！");
            return;
        }
        //判断表是否存在
        if (isExists(tableName)){
            System.out.println("表：" + tableName + "已经存在！！！");
            return;
        }
        //创建表描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        //
        for(String cf:cfn){
            ColumnFamilyDescriptor build1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            tableDescriptorBuilder.setColumnFamily(build1);
        }

        tableDescriptorBuilder.setCoprocessor("com.atguigu.Hbase_Comprocessor");
        TableDescriptor build = tableDescriptorBuilder.build();

        admin.createTable(build);
    }

    //删除表
    public static void deleteTable(String tableName) throws IOException {
        if (!isExists(tableName)){
            System.out.println("表：" + tableName +" 不存在！！！");
            return;
        }
        //disable
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //释放资源
    public static void closeNamespace() throws IOException {
        connection.close();
        admin.close();
    }



    public static void main(String[] args) throws IOException {
//        createNamespace("bigtable1");
        createTable("stu3","info");
//        System.out.println(isExists("stu2"));
//        closeNamespace();
    }
}
