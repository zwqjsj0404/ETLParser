package com.teradata.mds.etlparser.logrinser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.TxtFileHandler;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

class Consumer implements Runnable {                   // 任务执行者
	private final BlockingQueue srcQueue;
	private final BlockingQueue resultQueue;
	
	private final  String[] SQLTAGs;

	Consumer(BlockingQueue srcQueue, BlockingQueue resultQueue, String [] sqltags) {
		this.srcQueue = srcQueue;
		this.resultQueue = resultQueue;
		SQLTAGs = sqltags;
	}

	public void run() {
		try {
			while (true) {
				TxtFileHandler txtFile = (TxtFileHandler) srcQueue.take();
				if (RelaParser.FINISH_STRING.equals(txtFile.getSource())) {
					srcQueue.put(txtFile);
					break;
				}
				rinsing(txtFile);
			}
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
	}

	void rinsing(TxtFileHandler txtfile) {                // 执行清洗 的程序 现在只用 FileUtil 来进行日志清理
		FileUtil fu = new FileUtil(SQLTAGs);
		File file = txtfile.getFile();
		try {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			
			StopWatch watch = new Log4JStopWatch("LogRinser", txtfile.getFilename());
			StringBuffer sb = fu.processFile1(br); 			// 第一次清理
			br.close();
			fr.close();
			watch.lap("LogRinser : process1");
			String result = fu.processFile2(sb);        // 第二次清理
			watch.stop("LogRinser : process2");
			
			//写入结果
			TxtFileHandler tfh = new TxtFileHandler(txtfile.getSource()
												  , txtfile.getPath()
												  , txtfile.getFilename()
												  , result
												  , null);
			this.resultQueue.put(tfh);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return;
	}
}

