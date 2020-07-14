package com.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guo
 * @create 2020-06-28-16:39
 */
public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //添加列名和列类型
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("action");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * //遍历每一行数据,做炸开操作【多次写出操作】
     *     //[{"a":"a1"},{"b":"b1"},{"c":"c1"}]
     * @param args
     * @throws HiveException
     */
    public void process(Object[] args) throws HiveException {
        if (args.length <= 0){
            return;
        }

        if (args[0] == null){
            return;
        }

        //获取UDTF函数的输入数据
        String input = args[0].toString();
        //对输入的数据创建JSONArray数组
        JSONArray jsonArray = new JSONArray(input);
        //遍历数组
        for (int i = 0; i < jsonArray.length(); i++) {
            // protected final void forward(Object o)
            ArrayList<Object> result = new ArrayList<Object>();
            result.add(jsonArray.getString(i));
            forward(result);
        }


    }

    public void close() throws HiveException {

    }
}
