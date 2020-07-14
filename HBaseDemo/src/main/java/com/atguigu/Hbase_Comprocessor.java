package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Guo
 * @create 2020-06-23-20:34
 */
public class Hbase_Comprocessor implements RegionObserver, RegionCoprocessor {

    public static void main(String[] args) {
        System.out.println(" aaaa");
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {

        System.out.println("********进入协处理器");

        //1.获取配置信息
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //2.获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //3.获取表对象
        Table stu = connection.getTable(TableName.valueOf("stu"));

        //4.插入数据
        stu.put(put);

        //5.释放资源
        stu.close();
        connection.close();


        System.out.println("********退出协处理器");
    }
}
