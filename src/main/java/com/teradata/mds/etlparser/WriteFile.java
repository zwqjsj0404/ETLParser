package com.teradata.mds.etlparser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

public class WriteFile implements Runnable {
	private BlockingQueue txtFileQueue = null; // 待写入文件的内容队列
	private BlockingQueue targetQueue = null; // 后续操作的队列
	private String targetPath; // 目标路径

	public WriteFile(String targetPath, BlockingQueue txtFileQueue) {
		this.targetPath = targetPath;
		this.txtFileQueue = txtFileQueue;
		this.targetQueue = null;
	}

	public WriteFile(String targetPath, BlockingQueue txtFileQueue, BlockingQueue targetQueue) {
		this.targetPath = targetPath;
		this.txtFileQueue = txtFileQueue;
		this.targetQueue = targetQueue;
	}

	public void run() {
		try {
			while (true) {
				TxtFileHandler txtFile = (TxtFileHandler) txtFileQueue.take();
				if (this.targetQueue != null)
					targetQueue.put(txtFile);
				String sourceSys = txtFile.getSource();
				if (sourceSys.equals(RelaParser.FINISH_STRING)) {
					break;
				} else {
					StopWatch watch = new Log4JStopWatch("Write SQL File", txtFile.getFilename());
					try {
						writeFile(txtFile);
					} catch (IOException e) {
						e.printStackTrace();
					}
					watch.stop();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/*
	 * 向磁盘写入文件
	 */
	private void writeFile(TxtFileHandler txtFile) throws IOException {
		String source = txtFile.getSource();
		String filename = txtFile.getFilename();
		String path = txtFile.getPath();
		String content = txtFile.getContent();

		String fullpath = this.targetPath + RelaParser.FILE_SEPARATOR + source + path;
		File file = new File(fullpath);
		if (!file.exists()) {
			if (!file.mkdirs()) {
				throw new IOException("创建目录错误");
			}
		}
		String fullpathname = fullpath + filename;
		file = new File(fullpathname);

		FileWriter fw = new FileWriter(file);
		BufferedWriter bf = new BufferedWriter(fw);
		bf.write(content);
		bf.close();
		fw.close();
	}

}
