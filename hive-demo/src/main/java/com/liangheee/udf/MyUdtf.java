package com.liangheee.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * 模拟explode将数组炸裂，我们直接将字符串炸裂
 * @author Hliang
 * @create 2022-10-26 23:58
 */
public class MyUdtf extends GenericUDTF {

    // 因为有可能输出多个值，所以这里用的values来接收，比如Map的炸裂就是输出多个值
    private ArrayList<String> values = new ArrayList<>();
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 存放炸裂后得到数据的别名
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");
        // 炸裂后得到的值的类型检查器
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 每一行数据都会调用一次该方法
     * @param objects
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {
        String line = objects[0].toString();
        String[] split = line.split(",");
        for (String item : split) {
            values.clear();
            values.add(item);
            forward(values);
        }
    }

    /**
     * 关闭资源
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {

    }
}
