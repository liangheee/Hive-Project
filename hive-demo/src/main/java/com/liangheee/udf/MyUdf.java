package com.liangheee.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 模拟求字符串的长度
 * @author Hliang
 * @create 2022-10-26 22:52
 */
public class MyUdf extends GenericUDF {


    /**
     * 初始化，判断参数类型、个数等是否满足要求，指定返回值类型
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 1){
            throw new UDFArgumentException("参数个数不是1个！");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 进行具体的计算执行处理数据
     * 输入：一行一行的数据，有多少行数据就会执行多少次这个函数
     * 输出：一行一行的数据处理所得的结果
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String line = deferredObjects[0].get().toString();
        if(line == null){
            return 0;
        }
        return line.length();
    }

    /**
     * 用于explain打印HQL执行过程的方法，一般返回空字符串就可以了
     * @param strings
     * @return
     */
    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
