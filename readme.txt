一、文件说明：
	

二、源代码两个重要修改：
	（一）com.teradata.sqlparser.parser.SqlParser类：
			此类进行了一些修改，但我不知道我这里的SqlParser类是否和建行的一致，我把我的修改描述一下，建行同事可进行相应的修改  
		1、bteqBridge方法：
			(1)方法开始对sql_num_limit数值的初始化部分去掉了，此修改是为了屏蔽在SQL解析过程中的依靠数据库保存临时数据
			(2)删除了finally块的System.gc()调用
			(3)将主处理循环包到了Try的块中
		2、addRelation方法：
			将此方法原来生成Insert语句的方式修改为生成一个VO对象，利用该VO对象，可选择直接插库、生成insert文件或者生成fastload文件。
			
	（二）tableRelationParser类：此类的修改和SqlParser相同
	
三、配置文件说明，三个配置文件需要放到工作目录，不用打包到jar包中
	1、log4j.properties 日志配置文件
	2、tap.properties 数据库连接配置文件
	3、parser.properties 解析程序配置文件，配置文件中有各参数说明
		此配置文件中主要修改如下几个参数：
		(1)targetpath：输出用的目录
		(2)logrinser.source：日志存放目录
		(3)logrinser.perlcode：配置处理全部日志，还是有选择的处理某些编码的日志。
		(3)sqlparser.param：可从原来的配置文件中拷贝出来
		(4)sqlparser.writeDB：可设置是否直接写入数据库
	
四、运行命令
	java -Xms512m -Xmx1280m -jar etl-parser-1.0.0-jar-with-dependencies.jar
	
五、生成的文件说明
	1、report.txt   程序执行情况报告
	2、error_log.log   详细的错误记录
	3、performance_logs.log		详细的性能记录
	4、在配置的输出目录中会有清洗后的日志文件及生成的insert语句文件
	

	

				
	   