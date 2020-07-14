package com.atguigu.writer;

import com.atguigu.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author Guo
 * @create 2020-07-11-11:12
 */
public class EsWriterByBulk {
    public static void main(String[] args) throws IOException {
        //1.创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.创建配置信息
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        //3.设置配置信息
        jestClientFactory.setHttpClientConfig(build);
        //4.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.创建多个index对象
        Stu lisa = new Stu("Lisa", 22L);
        Stu jack = new Stu("jack", 23L);
        Stu chen = new Stu("chen", 35L);

        Index index1 = new Index.Builder(lisa).id("1006").build();
        Index index2 = new Index.Builder(jack).id("1007").build();
        Index index3 = new Index.Builder(chen).id("1008").build();

        //6.创建bulk对象
        Bulk.Builder builder = new Bulk.Builder().addAction(index1).addAction(index2).addAction(index3)
                .defaultIndex("stu").defaultType("_doc");

        Bulk bulk = builder.build();

        //7.执行批量插入数据操作
        jestClient.execute(bulk);

        //8.关闭资源
        jestClient.close();
    }
}
