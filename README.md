# HbaseSchool
· 发布版本中 vX.X.X (例如 v1.2.0) 适配 Hbase -1.2.5 分支  
· 发布版本中 wX.X.X (例如 w1.2.0) 适配 Hbase-2.0.1 分支

这是一个Hbase的Java操作框架，它能够实现Java对Hbase操作的快速实现，它由 HbaseTo 和 HbaseGo 两部分组成，其中  
- HbaseTo是一个工具，它可以导出一些 “*Bean.java” 文件和 "HbaseToTables.xml" 文件，这些文件是为了 HbaseGo 服务的。
- HbaseGo是一个 Java 的实现了 Hbase 数据增、删、改、查的开发框架，你只需要使用 HbaseTo 进行数据导出生成，然后将文件拷贝到你的工程之后稍作调整，再将HbaseGo架包导入工程就可以使用了。
- HbaseGoTest是一个测试演示Demo  
- HbaseGo和HbaseTo运行的时候都需要Hbase的Java库支持，不同版本的Hbase可能会有不同，具体使用的版本请参照发布时适配的版本
- 你可以在这里查看我用过的所有版本的Java Hbase 库，链接: https://pan.baidu.com/s/1qXEWOJM 密码: nijd
- [点击这里查看发布版本](https://github.com/Thonnn/HbaseSchool/releases)  

也许您只需要10分钟的的学习成本加上5分钟的配置成本就可以正式使用本框架了，HbaseGoTest 是一个演示，希望对你有用；
也许对于Hbase数据的操作可以变得更加简单疯狂；
也许 HbaseGo 能像 Mybatis 那样工作。
也许，我只是在期望也许。

## HbaseTo
你只需要在一台有Java JDK.8环境的机器上，使用 ``` java -jar "HbaseTo xxx.jar" ``` 命令运行，并按照发布版本的说明一步步执行就可以了

## HbaseGo
由于工程中使用了一部分JDK1.8的特性，所以使用 HbaseGo 需要至少JRE/JDK 1.8 以上版本支持。  
具体请参照发布时的说明

## HbaseGoTest
是一个演示测试Demo，希望对你有用

=====================================================================

# HbaseSchool
· Release version vX.X.X (eg. v1.2.0) adapted to the branch of Hbase-1.2.5.  
· Release version wX.X.X (eg. w1.2.0) adapted to the branch of Hbase-2.0.1.

HbaseSchool is a develop tool for Hbase. It can help developers to develop Java programs about Hbase quickly. It consists 2 parts of HbaseTo and HbaseGo : 
- HbaseTo is a tool which can export many "*Bean.java" files and "HbaseToTables.xml" file for HbaseGo's work.
- HbaseGo is a tool which can operate data for hbase, for example, it can add, delete, alter, search data for hbase. You need to use HbaseTo to export many files and copy them to your project and import HbaseGo to it for you work.  
- HbaseGoTest is test demo.  
- HbaseGo and HbaseTo need java libs about Hbase to run, different version HbaseGo and HbaseTo may need different version libs, you can find current verions libs in release notes.
- You can find many verison libs in [https://pan.baidu.com/s/1qXEWOJM](https://pan.baidu.com/s/1qXEWOJM)  password: nijd
- [Click here to Release](https://github.com/Thonnn/HbaseSchool/releases)  

Maybe you just need 10 minutes to learn these and 5 minutes to config it, HbaseGoTest is a demo, hope it useful.  
Maybe operate Hbase's data will comming to easy and crazy.  
Maybe HbaseGo can work well like Mybatis.  
Maybe, it just maybe for me.  
  
## HbaseTo
You can run this program in a computer which with java JDK1.8 envirment. Command ``` java -jar "HbaseTo xxx.jar" ``` to run. Config it step by step as the program explained until over.

## HbaseGo 
JDK1.8 is necessary.

## HbaseGoTest
This is a test demo.
