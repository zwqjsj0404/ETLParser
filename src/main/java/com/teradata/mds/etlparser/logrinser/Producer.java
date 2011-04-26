package com.teradata.mds.etlparser.logrinser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.TxtFileHandler;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

/**
 * @author qz185007
 *
 */
class Producer implements Runnable {              // 任务派发者 
	private final BlockingQueue queue;
	private final String sourcePath ;
	private Map files = new HashMap();		//key是perl脚本名称，value是File对象
	private String[] codes;
	
	private String fileCheckRegex = "";	//检验文件名称的正则表达式
	private String nameSeparator = "";	//切分文件名的分隔符
	private boolean hasTypeCode = false;	//是否含有4位类型编码
	private int codeOffset = 0;				//类型编码起始位置相对分隔符的偏移量
	
	Producer(BlockingQueue q, String home, String code) {
		queue = q;
		sourcePath = home;
		codes = code.split(";");
		try {
			this.initParam();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void initParam() throws IOException {
		InputStream is = new FileInputStream("parser.properties");
		Properties params = new Properties();
		params.load(is);
		
		this.fileCheckRegex = params.getProperty("log.name.checkrule", "");
		this.nameSeparator = params.getProperty("log.name.separator", "");
		String value = params.getProperty("log.name.hascode", "false");
		this.hasTypeCode = Boolean.valueOf(value).booleanValue();
		value = params.getProperty("log.name.codeidx", "0");
		this.codeOffset = Integer.parseInt(value);
		is.close();
	}

	public void run() {
		File file = new File(sourcePath);
		StopWatch watch = new Log4JStopWatch("Directory Scan");
		scandir(file);
		watch.stop();
		
		RelaParser.setTotalFiles(files.size());
		Iterator iter = files.values().iterator();
		while(iter.hasNext()) {
			file = (File) iter.next();
			try {
				//拆解出文件的相对路径
				String filename = file.getName();
				String fullPath = file.getAbsolutePath();
				
				
				String relaPath = fullPath.substring(this.sourcePath.length());	//去掉前导原始目录
				int filenameIdx = relaPath.indexOf(filename);
				relaPath = relaPath.substring(0, filenameIdx);
				
				TxtFileHandler txtFile = new TxtFileHandler("logParser", relaPath, filename, null, file);
				queue.put(txtFile);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * 扫描目录，将最终待处理文件加入filesMap中
	 */
	private void scandir(File sourceFile) {
		if(sourceFile.isDirectory()) {
			File[] children = sourceFile.listFiles();
			for(int i=0; i<children.length; i++) {
				if(children[i].isDirectory()) {
					scandir(children[i]);
				} else if(this.chkFileName(children[i].getName())) {
					putIfNecessary(children[i]);
					
				}
			}
		}
	}
	
	/*
	 * 判断是否需要将该文件加入到HashMap中
	 */
	private void putIfNecessary(File file) {
		String filename = file.getName();
		
		int plIdx = filename.indexOf(this.nameSeparator);
		if(plIdx < 0) return;
		
		String perlName = filename.substring(0, plIdx + this.nameSeparator.length());
		
		if(this.hasTypeCode) {
			//判断类型代码
			String fileCode = filename.substring(plIdx + this.codeOffset, plIdx + this.codeOffset + 4);
			boolean match = false;
			for(int i=0; i<codes.length; i++) {
				String code = codes[i];
				//需要排除的
				if(code.startsWith("-")) {
					String trueCode = code.substring(1);
					if(fileCode.equals(trueCode)) break;
				}
				//添加的
				if(code.equals("*") || fileCode.equals(code)) {
					match = true;
					break;
				}
			}
			if(!match)	return;
		}
		File lastFile = (File) this.files.get(perlName);
		if(lastFile == null || lastFile.lastModified() < file.lastModified()) {
			if(lastFile != null) {
				//TODO：增加配置。判断文件大小，如果相差2倍以上，保留大的
				long lastLen = lastFile.length();
				long newLen = file.length();
				if(newLen * 2 < lastLen) return;
			}
			this.files.put(perlName, file);
		}
	}
	
	/**
	 * 文件名是小写,且以log结束的文件为有效文件.
	 * @param fileName
	 * @return
	 */
	private boolean chkFileName(String fileName){
		//Pattern p = Pattern.compile("[a-z0-9]{1,}(.*?)log");
		Pattern p = Pattern.compile(this.fileCheckRegex);
		Matcher m = p.matcher(fileName);
		return m.matches(); 
	}
}
