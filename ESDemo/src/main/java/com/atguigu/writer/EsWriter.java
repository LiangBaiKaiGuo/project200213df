package com.atguigu.writer;

import com.atguigu.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author Guo
 * @create 2020-07-11-9:17
 */
public class EsWriter {
    public static void main(String[] args) throws IOException {
        //1.创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.创建配置信息
        HttpClientConfig builder = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        //3.设置配置信息
        jestClientFactory.setHttpClientConfig(builder);
        //4.获取JestClient
        JestClient jestClient = jestClientFactory.getObject();
        //5.构建ES插入数据对象
        Stu shixiong = new Stu("shixiong", 18L);
        Index index = new Index.Builder(shixiong).index("stu").type("_doc").id("1005").build();

        //6.触发插入数据操作
        jestClient.execute(index);

        //7.关闭资源
        jestClient.close();
    }
}
