package com.teradata.mds.etlparser.logrinser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.TxtFileHandler;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

/**
 * <pre>
 * Title: TeraDate BTEQ Script Log File Rinser
 * Description: 对　ＢＴＥＱ　日志的无用信息进行过滤，只留可执行的ＳＱＬ
 * </pre>
 * 
 * @author QingZhang@teradata.com
 * @version 1.00.00
 * 
 *          <pre>
 * 修改记录
 *    修改后版本:     修改人：  修改日期:     修改内容:
 * </pre>
 */
public class LogRinser implements Runnable {
	private int threadCount = 5; // 清洗任务线程数量
	private BlockingQueue targetQueue; // 发送最终结果的队列
	private String sourcePath = null; // 源路径
	private String[] SQLTAGs = null; // SQL关键字
	private String perlCode;

	public LogRinser(String sourcePath, BlockingQueue sqlQueue, int threads, String code) {
		this.sourcePath = sourcePath;
		this.threadCount = threads;
		this.targetQueue = sqlQueue;
		this.perlCode = code;
		ReadSqlTags();
	}

	public void ReadSqlTags() {
		String[] SQLTAGdefault = { // 读不到 SQLTAGS.TXT 才用默认的。
		"ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AVG", "BEGIN", "BETWEEN",
				"BY", "CASE", "CAST", "CHARACTER", "COALESCE", "COMMENT",
				"COMMIT", "COUNT", "CREATE", "CROSS", "DEL", "DELETE", "DESC",
				"DISTINCT", "DROP", "ELSE", "END", "END)", "EXISTS", "FROM",
				"FULL", "GROUP", "HAVING", "IN", "IN(", "INDEX", "INNER",
				"INSERT", "INTERSECT", "INTO", "JOIN", "LEFT", "LIKE", "LOCK",
				"LOWER", "MAX", "MIN", "MINUS", "MODIFY", "NAMED", "NOT",
				"NULL", "ON", "OR", "ORDER", "OUTER", "PERCENT", "POSITION",
				"PRIMARY", "QUALIFY", "RENAME", "REPLACE", "REVOKE", "RIGHT",
				"ROLE", "ROLLBACK", "SAMPLE", "SEL", "SELECT", "SET", "SHOW",
				"SOME", "SUBSTR", "SUBSTRING", "SUM", "TABLE", "THEN", "TITLE",
				"TO", "TRIM", "UNION", "UNIQUE", "UPDATE", "UPPER", "VALUES",
				"VIEW", "WHEN", "WHERE", "WHERE(", "WITH", "(", ")", ",", ";" };
		FileReader fr = null;
		BufferedReader br = null;
		String infile = "SQLTAGS.TXT";
		String[] t1 = null;
		try {

			fr = new FileReader(infile);
			br = new BufferedReader(fr);
			String line = " ";
			ArrayList al = new ArrayList();
			for (;;) {
				line = br.readLine();
				if (line == null)
					break;
				line = line.trim();
				al.add(line);
			}
			if (al.size() > 0)
				t1 = new String[al.size()];
			for (int i = 0; i < al.size(); i++) {
				t1[i] = (String) al.get(i);
			}

			SQLTAGs = t1;

		} catch (FileNotFoundException e) { // 读不到 SQLTAGS.TXT 就用默认的。
			SQLTAGs = SQLTAGdefault;
		} catch (IOException e) {
			SQLTAGs = SQLTAGdefault;
			e.printStackTrace();
		} finally {
			if (null != br)
				try {
					br.close();
				} catch (IOException e) {
				}
			if (null != fr)
				try {
					fr.close();
				} catch (IOException e) {
				}
		}
	}

	public void run() {
		//中间临时队列，该队列中存储源文件
		BlockingQueue srcQueue = new ArrayBlockingQueue(threadCount * 4);
		
		//创建源目录扫描线程，将待处理文件的全路径名称加入srcQueue
		Thread pt = new Thread(new Producer(srcQueue, sourcePath, perlCode), "File Search Thread");
		pt.start();

		try {
			Thread.sleep(1);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		//创建日志过滤线程，从srcQueue中读取文件名，处理后添加到targetQueue中
		Thread[] ccp = new Thread[threadCount];

		for (int i = 0; i < threadCount; i++) {
			ccp[i] = new Thread(new Consumer(srcQueue, targetQueue, SQLTAGs), "Rinser Thread");
			ccp[i].start();
		}

		//等待生产者线程结束
		try {
			pt.join();
			srcQueue.put(new TxtFileHandler(RelaParser.FINISH_STRING,null, null, null, null));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//等待清洗线程全部结束
		for (int i = 0; i < threadCount; i++) {
			try {
				ccp[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//自身结束
		return;
	}
}
