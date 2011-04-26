package com.teradata.mds.etlparser.relation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.teradata.db.DBQueryEngine;
import com.teradata.mds.etlparser.DBUtil;
import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.sqlparse.SQLParserLoader;
import com.teradata.tap.system.query.QueryException;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

/*
 * 解析后的数据写文件线程
 */
public class RelationDataWriter implements Runnable {
	private static String insertFileName = "insert.data";
	private static String relaTFastloadName  = "fast_rela_t.data";
	private static String relaCFastloadName = "fast_rela_c.data";
	
	private final String targetpath;
	private final String subsys;
	private final BlockingQueue dataqueue;
	
	private boolean writeDB = false;
	
	
	public RelationDataWriter(String targetpath, String subsys, BlockingQueue dataqueue
							, boolean writeDB) {
		this.dataqueue = dataqueue;
		this.targetpath = targetpath;
		this.subsys = subsys;
		
		this.writeDB = writeDB;
	}

	public void run() {
		//FIXME: 创建文件
		try {
			String pathname = targetpath + RelaParser.FILE_SEPARATOR + subsys + RelaParser.FILE_SEPARATOR;
			File path = new File(pathname);
			if(!path.mkdirs()) {
				System.out.println("无法创建数据文件目录！");
			}
			
			//insert file
			String insertfilename = pathname + insertFileName;
			FileWriter insertFW = new FileWriter(insertfilename);
			BufferedWriter insertBW = new BufferedWriter(insertFW);
			
			//fastload file
			String fastCFilename = pathname + relaCFastloadName;
			FileWriter fastCFW = new FileWriter(fastCFilename);
			BufferedWriter fastCBW = new BufferedWriter(fastCFW);
			String fastTFilename = pathname + relaTFastloadName;
			FileWriter fastTFW = new FileWriter(fastTFilename);
			BufferedWriter fastTBW = new BufferedWriter(fastTFW);
			
			//数据库连接
			DBQueryEngine engine = null;
			try {
				// 初始化数据库连接
				engine = DBUtil.getEngine();
				engine.setAutoCommit(false);
			} catch (QueryException e) {
				e.printStackTrace();
				engine = null;
			}

			
			while(true) {
				SQLParseResult result = (SQLParseResult) dataqueue.take();
				String filename = result.getLogfileName();
				if(filename.equals(RelaParser.FINISH_STRING)) break;
				List relas = result.getRelas();
				if(relas.size() == 0) {
					break;
				} else {
					StopWatch watch = new Log4JStopWatch("Begin wrtie file", filename);
					writeInsertFile(insertBW, relas);
					watch.lap("Write Insert File", filename);
					writeFastloadFile(fastCBW, fastTBW, relas);
					watch.lap("Write Fastload File", filename);
					writeDB(engine, relas);
					watch.lap("Insert into DB", filename);
				}
			}
			
			insertBW.close();
			insertFW.close();
			
			fastCBW.close();
			fastCFW.close();
			fastTBW.close();
			fastTFW.close();
			
			if(engine != null) engine.removeDB();
		} catch(InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void writeInsertFile(BufferedWriter bw, List result) {
		int count = result.size();
		try {
			for (int i = 0; i < count; i++) {
				DataRelaVO vo = (DataRelaVO) result.get(i);
				String line = vo.getInsertString();
				bw.write(line);
				bw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void writeFastloadFile(BufferedWriter fastCBW, BufferedWriter fastTBW, List result) {
		int count = result.size();
		try {
			for (int i = 0; i < count; i++) {
				DataRelaVO vo = (DataRelaVO) result.get(i);
				String line = vo.getFastloadString();
				if(vo instanceof DataRelaColumnVO) {
					fastCBW.write(line);
					fastCBW.write("\n");
				} else {
					fastTBW.write(line);
					fastTBW.write("\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 写入数据库
	 */
	private void writeDB(DBQueryEngine engine, List result) {
		if(!this.writeDB) return;
		int count = result.size();
			
		try {
			for(int i=0; i<count; i++) {
				DataRelaVO vo = (DataRelaVO) result.get(i);
				String line = vo.getInsertString();
				engine.execute(line);
			}
			engine.commit();
		} catch(QueryException e) {
			e.printStackTrace();
		}
	}
	
	public static String convertToFastload(String content, int length) {
		StringBuffer result = new StringBuffer();
		
		if(content == null) content = "";
		byte[] bytes = content.getBytes();
		int bytelen = bytes.length;
		int strlen = content.length();
		if(bytelen > length) {
			//异常情况
			result.append(content.substring(0, strlen - (bytelen - length)));
		} else {
			result.append(content);
			
			for(int i=length - bytelen; i>0; i--) {
				result.append(' ');
			}
		}
		return result.toString();
	}
	
	public static void main(String[] args) {
		String str = null;
		String result = null;
		
		str = "adfadfadf";
		result = convertToFastload(str, 3);
		System.out.println(result);
		
		str = "ad我们fadfadf";
		result = convertToFastload(str, 7);
		System.out.println(result);

		str = "ad我们fadfadf";
		result = convertToFastload(str, 5);
		System.out.println(result);
	
	}
	
}
