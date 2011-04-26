package com.teradata.mds.etlparser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.perf4j.StopWatch;

import com.teradata.mds.etlparser.logrinser.LogRinser;
import com.teradata.mds.etlparser.sqlparse.SQLParserLoader;
import com.teradata.mds.etlparser.viewparse.ViewParserLoader;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

/**
 * 元数据关系解析器
 * 
 * @author MarkDong(md186003)
 * 
 */
public class RelaParser {
	public static String FILE_SEPARATOR = "";
	public static final String FINISH_STRING = "ALL_FINISHED";
	public static final int HASHMAP_CAPACITY = 512;
	
	// 基本配置
	private static String targetPath = null;
	private static boolean logParser = false; // 是否进行SQL解析
	private static boolean viewParser = false;	//是否进行View解析
	private static int cupNums = 2;

	private static int sqlQueueLength = 10; // 中间结果队列长度

	// 日志清洗任务配置
	private static String logSourcePath = null;
	private static String logPerlCode = null;
	private static int logRinserThreads = 1;

	// SQL解析任务配置
	private static String paramSql = null;
	private static int sizeThreashold = 20480;
	private static boolean writeDB = false;
	private static int relaQueueSize = 50;
	private static String sql_postfix = "";
	
	
	//View解析配置
	private static String postfix;

	//日志处理记录基本信息
	private static int totalFiles = 0;
	private static int done = 0;
	private static int sucessFiles = 0;
	private static int failedFiles = 0;
	private static List failedList = new ArrayList(100);
	
	/*
	 * 设置文件总数
	 */
	public synchronized static void setTotalFiles(int total) {
		totalFiles = total;
		System.out.println("待处理数量：" + totalFiles);
	}
	
	/*
	 * 成功文件
	 */
	public synchronized static void addSucess() {
		sucessFiles += 1;
		done += 1;
		if(done % 50 ==0 ) {
			System.out.println("已处理文件数：" + done);
		}
	}
	
	/*
	 * 失败文件
	 */
	public synchronized static void addFailed(String name) {
		failedFiles += 1;
		failedList.add(name);
		done += 1;
		if(done % 50 ==0 ) {
			System.out.println("已处理文件数：" + done);
		}
	}
	
	public static void main(String args[]) {
		FILE_SEPARATOR = System.getProperty("file.separator");
		PropertyConfigurator.configure("log4j.properties");

		initParams(args);
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		String reportname = "./report_";
		reportname = reportname + getCurrentDate() + ".txt";
    
		try {
			File report = new File(reportname);
			fw = new FileWriter(report);
			bw = new BufferedWriter(fw);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		LogParse(bw);
		ViewParse(bw);
		
		try {
			if(bw != null) bw.close();
			if(fw != null) fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static String getCurrentDate() {
	  String result = "";
	  Calendar calendar = Calendar.getInstance();
	  result = result + calendar.get(Calendar.YEAR);
	  String month = String.valueOf(calendar.get(Calendar.MONTH) + 1);
	  if(month.length() == 1) month = "0" + month;
	  result += month;
	  String day = String.valueOf(calendar.get(Calendar.DATE));
	  if(day.length() == 1) day = "0" + day;
	  result += day;
	  return result;
	}
	
	/**
	 * View解析主流程
	 */
	private static void ViewParse(BufferedWriter bw) {
		if(!viewParser) return;
		
		totalFiles = 0;
		done = 0;
		sucessFiles = 0;
		failedFiles = 0;
		failedList.clear();
		
		StopWatch mainWatch = new StopWatch("View Parse");
		
		Thread viewParse = new Thread(new ViewParserLoader(targetPath, cupNums, relaQueueSize, writeDB, postfix, sql_postfix), "View Parse Loader");
		viewParse.start();
		try {
			viewParse.join();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		mainWatch.stop();
		//打印最终信息
		try {
			long elapsedTime = mainWatch.getElapsedTime();
			long hours = elapsedTime / (1000 * 3600);
			long minutes = (elapsedTime / (1000 * 60)) % 60;
			long seconds = elapsedTime / 1000 % 60;
			Date start = new Date(mainWatch.getStartTime());
			Date end = new Date(mainWatch.getStartTime() + elapsedTime);
			bw.write("==========视图处理结果信息==========\n");
			bw.write("开始运行时间：" + start + "\n");
			bw.write("运行结束时间：" + end + "\n");
			bw.write("持续运行时间：" + hours + "小时" + minutes + "分" + seconds +"秒\n");
			bw.write("处理总数：" + totalFiles + "\n");
			bw.write("成功：" + sucessFiles + "; 失败：" + failedFiles + "\n");
			bw.write("失败列表：" + "\n");
			for(int i=0; i<failedList.size(); i++) {
				bw.write("\t" + failedList.get(i) + "\n");
			}
			bw.write("==================================\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * log解析主流程
	 */
	private static void LogParse(BufferedWriter bw) {
		if(!logParser) return;
		
		StopWatch mainWatch = new StopWatch("Log Parse");

		// 创建中间队列，此队列中只保存TxtFileHandler对象
		BlockingQueue logQueue = new ArrayBlockingQueue(sqlQueueLength);
		BlockingQueue sqlQueue = new ArrayBlockingQueue(sqlQueueLength);


		List producers = new ArrayList();
		Thread logRinserThread = new Thread(new LogRinser(logSourcePath, logQueue, logRinserThreads, logPerlCode), "LogRinser");
		producers.add(logRinserThread);
		logRinserThread.start();

		//启动写入文件的线程
		Thread writeFileThread = null;
		// 创建一个写文件的线程
		writeFileThread = new Thread(new WriteFile(targetPath, logQueue, sqlQueue), "WriteFile");
		
		writeFileThread.start();
		
		List consumers = new ArrayList();
		//启动SQL解析线程
		Thread parseLoaderThread = new Thread(new SQLParserLoader(targetPath, sqlQueue, cupNums
												, paramSql, sizeThreashold
												, writeDB, relaQueueSize, sql_postfix), "ParserLoader");
		parseLoaderThread.start();
		
		consumers.add(parseLoaderThread);
		
		//等待生产者线程停止
		try {
			for (int i = 0; i < producers.size(); i++) {
				Thread producer = (Thread) producers.get(i);
				producer.join();
			}
			logQueue.put(new TxtFileHandler(FINISH_STRING, null, null, null, null));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//等待消费者线程停止
		try {
			for (int i=0; i<consumers.size(); i++) {
				Thread consumer = (Thread) consumers.get(i);
				consumer.join();
			}
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
		
		mainWatch.stop();
		
		//打印最终信息
		try {
			
			long elapsedTime = mainWatch.getElapsedTime();
			long hours = elapsedTime / (1000 * 3600);
			long minutes = (elapsedTime / (1000 * 60)) % 60;
			long seconds = elapsedTime / 1000 % 60;
			Date start = new Date(mainWatch.getStartTime());
			Date end = new Date(mainWatch.getStartTime() + elapsedTime);
			bw.write("==========日志处理结果信息==========\n");
			bw.write("开始运行时间：" + start + "\n");
			bw.write("运行结束时间：" + end + "\n");
			bw.write("持续运行时间：" + hours + "小时" + minutes + "分" + seconds +"秒\n");
			bw.write("处理总文件数：" + totalFiles + "\n");
			bw.write("成功：" + sucessFiles + "; 失败：" + failedFiles + "\n");
			bw.write("失败文件列表：" + "\n");
			for(int i=0; i<failedList.size(); i++) {
				bw.write("\t" + failedList.get(i) + "\n");
			}
			bw.write("==================================\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void initParams(String[] args) {
		if (!initConfigFromFile()) {
			System.out.println("Got error when initial from 'parser.properties' file.");
			System.exit(-1);
		}

		if (!initConfigFromArgs(args)) {
			
		}
		if (!checkConfig()) {

		}
	}

	/*
	 * 从配置文件读取参数
	 */
	private static boolean initConfigFromFile() {
		boolean success = true;
		try {
			InputStream is = new FileInputStream("parser.properties");
			Properties params = new Properties();
			params.load(is);
			String value = null;

			value = params.getProperty("logparser", "false");
			logParser = Boolean.valueOf(value).booleanValue();
			value = params.getProperty("viewparser", "false");
			viewParser = Boolean.valueOf(value).booleanValue();
			value = params.getProperty("cpunums", "2");
			cupNums = Integer.parseInt(value);
			
			value = params.getProperty("logrinser.queue", "10");
			sqlQueueLength = Integer.parseInt(value);

			targetPath = params.getProperty("targetpath", null);

			value = params.getProperty("logrinser.threads", "1");
			logRinserThreads = Integer.parseInt(value);
			logSourcePath = params.getProperty("logrinser.source", null);
			logPerlCode = params.getProperty("logrinser.perlcode", "");

			value = params.getProperty("sqlparser.sizethreshold", "20480");
			sizeThreashold = Integer.parseInt(value);
			paramSql = params.getProperty("sqlparser.param");
			value = params.getProperty("sqlparser.relationQueueSize", "50");
			relaQueueSize = Integer.parseInt(value);
			writeDB = Boolean.valueOf(params.getProperty("sqlparser.writeDB", "false")).booleanValue();
			postfix = params.getProperty("viewparser.postfix", "");
			
			sql_postfix = params.getProperty("sqlparser.postfix", "");
			is.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			success = false;
		} catch (IOException e) {
			e.printStackTrace();
			success = false;
		}
		
		return success;
	}

	/*
	 * 从命令行读取参数
	 */
	private static boolean initConfigFromArgs(String args[]) {
		return true;
	}

	/**
	 * 检查所有配置信息的合法性
	 */
	private static boolean checkConfig() {
		return true;
	}
}
