### Hive自定义函数
#### 1 用户自定义的函数类型有UDF、UDAF、UDTF三种，可以参见官方文档https://cwiki.apache.org/confluence/display/Hive/HivePlugins
#### 2. 自定义函数的步骤
2.1 引入pom依赖
```$xslt
<dependencies>
	<dependency>
		<groupId>org.apache.hive</groupId>
		<artifactId>hive-exec</artifactId>
		<version>3.1.3</version>
	</dependency>
</dependencies>
```
2.2 继承Hive提供的类
```$xslt
UDF基类：org.apache.hadoop.hive.ql.udf.generic.GenericUDF
UDTF基类：org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
```
2.3 实现其中的抽象方法
2.4 在Hive的命令行窗口创建函数
```$xslt
临时函数
    添加jar：add jar linux_jar_path
    创建函数：create temporary function [db_name].function_name as "class_name";
    删除函数：drop temporary function [db_name].function_name;
永久函数
    创建函数：create function [db_name].function_name as "class_name" using jar hdfs_jar_path;  注意永久函数的jar包必须在hdfs上
    删除永久函数：drop function [db_name].function_name;
```
#### 3.案例
本Demo中提供了求字符串长度UDF、根据逗号炸裂字符串的UDTF以及炸裂Map集合的UDTF供参考
