# HbaseSchool
这是一个Hbase的Java操作框架，它能够实现Java对Hbase操作的快速实现，其中  
- HbaseTo是一个数据表导出供HbaseGo使用的xml和JavaBean的工具；  
- HbaseGo是一个Hbase操作框架，就像Mybatis那样；  
- HbaseGoTest是一个测试演示Demo  
- HbaseGo和HbaseTo运行的时候都需要Hbase的Java库支持，不同版本的Hbase可能会有不同，具体使用的版本请参照发布时适配的版本
- 你可以在这里查看我用过的所有版本的Java Hbase 库，链接: https://pan.baidu.com/s/1qXEWOJM 密码: nijd
- [点击这里查看发布版本](https://github.com/Thonnn/HbaseSchool/releases)

## HbaseTo
你只需要在一台有Java环境的机器上，使用 ``` java -jar "HbaseTo xxx.jar" ``` 命令运行，并按照发布版本的说明一步步执行就可以了

## HbaseGo
由于工程中使用了一部分JDK1.8的特性，所以使用 HbaseGo 需要至少JRE/JDK 1.8 以上版本支持。
具体请参照发布时的说明

## HbaseGoTest
是一个演示测试Demo，希望对你有用
