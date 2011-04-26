package com.teradata.mds.etlparser.sqlparse;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.teradata.db.DBQueryEngine;
import com.teradata.mds.etlparser.DBUtil;
import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.TxtFileHandler;
import com.teradata.mds.etlparser.relation.SQLParseResult;
import com.teradata.sqlparser.parser.SqlParser;
import com.teradata.sqlparser.parser.tableRelationParser;
import com.teradata.tap.system.query.QueryException;

public class SQLParseThread implements Runnable {
	private final static Logger logger = Logger.getLogger(SQLParseThread.class);
	
	private SQLParserLoader loader;
	private DBQueryEngine engine;
	public SQLParseThread(SQLParserLoader loader) {
		this.loader = loader;
	}

	public void run() {
		try {
			engine = DBUtil.getEngine();
			while (true) {
				
				TxtFileHandler sqlFile = (TxtFileHandler) loader.sqlQueue.take();
				String source = sqlFile.getSource();
				if (RelaParser.FINISH_STRING.equals(source)) {
					loader.sqlQueue.put(sqlFile);
					break;
				}

				List relaData = parseScriptFile(sqlFile);
				
				if(relaData.size() > 0) {
					SQLParseResult result = new SQLParseResult(sqlFile.getFilename(), relaData);
					loader.relationQueue.put(result);
				}
			}
			engine.removeDB();

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (QueryException e) {
			e.printStackTrace();
		}

	}

	private List parseScriptFile(TxtFileHandler txtFile) {
		List result = new ArrayList();

		String content = txtFile.getContent();
		//String filename = txtFile.getFilename();
		//int index = filename.indexOf(".pl");
		//if(index != -1 ) filename = filename.substring(0, index+3);
		SQLExtractUtil util = new SQLExtractUtil(content, loader.templates);
		List sqlList;
		StopWatch watch = null;

		try {
			watch = new Log4JStopWatch("SQL Parser");
			sqlList = util.extractSqlList();
			watch.lap("SQL Parser : Split SQL", txtFile.getPerlFilename());

			SqlParser sqlparser = null;
			if (content.length() > loader.sizeThreshold) {
				//Big file
				sqlparser = new SqlParser(engine, "", loader.columnMap, loader.tableMap, loader.scriptMap);
			} else {
				//Small file
				sqlparser = new SqlParser("", loader.columnMap, loader.tableMap, loader.scriptMap);
			}
			
			List result1 = sqlparser.bteqBridge(sqlList, txtFile.getPerlFilename());
			if(content.length() > loader.sizeThreshold) {
				watch.lap("SQL Parser : Big SQL", txtFile.getPerlFilename());
			} else {
				watch.lap("SQL Parser : Small SQL", txtFile.getPerlFilename());
			}
			
			tableRelationParser sqlparser2 = new tableRelationParser("", loader.tableMap, loader.scriptMap);
			List result2 = sqlparser2.bteqBridge(sqlList, txtFile.getPerlFilename());
			watch.lap("SQL Parser : Parse Table Relation", txtFile.getPerlFilename());
			
			if(result1 != null) result.addAll(result1);
			if(result2 != null) result.addAll(result2);
			
			RelaParser.addSucess();
		} catch (Exception e) {
			watch.lap("SQL Parser : Error!", txtFile.getPerlFilename());
			logger.warn("SQL Parser : Error! File = " + txtFile.getPerlFilename(), e);
			RelaParser.addFailed(txtFile.getPerlFilename());
		}
		
		return result;
	}

}
