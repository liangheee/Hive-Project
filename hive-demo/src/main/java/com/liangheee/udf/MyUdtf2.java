package com.liangheee.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 模拟map集合的炸裂
 * @author Hliang
 * @create 2022-10-27 0:16
 */
public class MyUdtf2 extends GenericUDTF {

    // 因为有可能输出多个值，所以这里用的values来接收，比如Map的炸裂就是输出多个值
    private transient final Object[] forwardMapObj = new Object[2];
    private transient ObjectInspector inputOI = null;
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 存放炸裂后得到数据的别名
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("key");
        fieldNames.add("value");
        // 炸裂后得到的值的类型检查器
        // 获取结构体的所有属性
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        // 创建集合手机结构体的所有属性
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        // 遍历结构体的所有属性
        for (StructField structField : allStructFieldRefs) {
            // 获取每一个属性对应的类型检查器
            ObjectInspector fieldObjectInspector = structField.getFieldObjectInspector();
            inputOI = fieldObjectInspector;
            fieldOIs.add(((MapObjectInspector)inputOI).getMapKeyObjectInspector());
            fieldOIs.add(((MapObjectInspector)inputOI).getMapValueObjectInspector());
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 每一行数据都会调用一次该方法
     * @param objects
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {
        Map<?,?> map = (Map<?, ?>) objects[0];
        for(Map.Entry<?,?> entry : map.entrySet()){
            forwardMapObj[0] = entry.getKey();
            forwardMapObj[1] = entry.getValue();
            forward(forwardMapObj);
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
