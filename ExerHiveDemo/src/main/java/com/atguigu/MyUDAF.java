package com.atguigu;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/** 传入一个字符串，按照传入的分隔符切分，并输出切分后的单词
 * @author Guo
 * @create 2020-06-27-8:22
 */
public class MyUDAF extends GenericUDTF {

    //输出集合
    private ArrayList<String> outlist = new ArrayList<String>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        ArrayList<String> fileNames = new ArrayList<String>();

        ArrayList<ObjectInspector> fileTypes = new ArrayList<ObjectInspector>();
        //约定输出列的名称
        fileNames.add("word");
        //约定输出列的类型
        fileTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fileNames,fileTypes);
    }

    public void process(Object[] args) throws HiveException {
        //判断传入的参数，是否等于2
        if (args == null || args.length != 2){
            new HiveException("Input Args length Error!!!");
        }
        //获取原数据
        String lineword = args[0].toString();
        //获取分隔符
        String splitKey = args[1].toString();
        //用分隔符切分传入的单词
        String[] words= lineword.split(splitKey);

        for (String word : words) {
            //集合为复用，清空集合
            outlist.clear();

            //将切分后的单词添加到集合中
            outlist.add(word);

            //将集合内容写出
            forward(outlist);
        }

    }

    public void close() throws HiveException {


    }
}
