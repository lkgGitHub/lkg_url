## org.apache.spark.rpc.RpcEnvStoppedException: RpcEnv already stopped异常

物理内存或者虚拟内存分配不够，Yarn直接杀死进程，需要禁止内存检查
编辑Hadoop中的etc/hadoop/yarn-site.xml文件，添加如下配置

```xml
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
```
