package com.atguigu;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author Guo
 * @create 2020-06-26-21:59
 */
public class MyUDF extends GenericUDF {

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //判断参数个数
        if (arguments == null || arguments.length == 0){
            new UDFArgumentLengthException("Input arg lenth Error!!!");
        }
        //判断参数类型
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            new UDFArgumentTypeException(0,"Input agr type Error");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        //取出参数
        Object o = arguments[0].get();
        return o.toString().length();
    }

    public String getDisplayString(String[] children) {
        return null;
    }
}
