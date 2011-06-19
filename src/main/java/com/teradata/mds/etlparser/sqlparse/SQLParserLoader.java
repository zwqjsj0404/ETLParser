package com.teradata.mds.etlparser.sqlparse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.teradata.db.DBQueryEngine;
import com.teradata.mds.etlparser.DBUtil;
import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.relation.RelationDataWriter;
import com.teradata.mds.etlparser.relation.SQLParseResult;
import com.teradata.tap.system.query.QueryException;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

public class SQLParserLoader implements Runnable {
	private final int threadCount;
	private final String paramSql;
	
	

	BlockingQueue sqlQueue;		//SQL文件队列
	HashMap columnMap;			
	HashMap tableMap;
	HashMap scriptMap;
	int sizeThreshold;
	Vector templates;
	String targetpath;
	boolean writeDB;
	String postfix;
	
	BlockingQueue relationQueue;	//存放解析后的数据的队列

	/**
	 * SQL解析主线程
	 * @param targetpath 目标路径，写入解析后的数据文件
	 * @param sqlQueue sql文件队列
	 * @param threads 解析线程数
	 * @param paramSql 
	 * @param sizeThreashold 区分大小文件的阈值
	 */
	public SQLParserLoader(String targetpath, BlockingQueue sqlQueue
			             , int threads, String paramSql
			             , int sizeThreashold, boolean writeDB, int resultqueuesize
			             , String postfix) {
		this.sqlQueue = sqlQueue;
		this.threadCount = threads;
		this.paramSql = paramSql;
		this.sizeThreshold = sizeThreashold;
		this.targetpath = targetpath;
		this.postfix = postfix;
		
		this.writeDB = writeDB;
		
		if(resultqueuesize <= 50)	resultqueuesize = 50;
		this.relationQueue = new ArrayBlockingQueue(resultqueuesize);
	}

	public void run() {
		columnMap = getColumnMap();
		tableMap = getTableMap();
		scriptMap = getScriptMap();
		templates = readTemplate();
		
		//创建解析线程
		List threads = new ArrayList();
		for(int i=0; i<this.threadCount; i++) {
			Thread parseThread = new Thread(new SQLParseThread(this), "ParseThread");
			threads.add(parseThread);
			parseThread.start();
		}
		
		//创建写文件线程
		Thread writeDBThread = new Thread(new RelationDataWriter(this.targetpath
																, "SQLParser"
																, this.relationQueue
																, this.writeDB)
										, "WriteRelaData");
		writeDBThread.start();
		
		//等待解析线程结束
		try {
			for (int i = 0; i < threads.size(); i++) {
				Thread parseThread = (Thread) threads.get(i);
				parseThread.join();
			}
		
			//为写文件线程发送结束通知
			this.relationQueue.put(new SQLParseResult(RelaParser.FINISH_STRING, null));
			writeDBThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
		
	}

	private HashMap getScriptMap() {
		String sql = "SELECT SCRIPT_ID " +
				",UPPER(SCRIPT_NAME) " +
				"FROM M05_ETL_SCRIPT " +
				"WHERE END_DT='2999-12-31 00:00:00'";

		HashMap map = new HashMap(RelaParser.HASHMAP_CAPACITY);
		
		List cols = null;
		try {
			DBQueryEngine engine = DBUtil.getEngine();
			cols = engine.getResultList(sql);
		} catch (QueryException e) {
			e.printStackTrace();
		}
		if (cols != null) {
			for (Iterator iterator = cols.iterator(); iterator.hasNext();) {
				Map col = (Map) iterator.next();
				String name = (String) col.get("SCRIPT_NAME");
				String id = (String) col.get("SCRIPT_ID");
				map.put(name, id);
			}
		}
		
		return map;
	}
	
	private HashMap getColumnMap() {
//		String sql = "SELECT DATABASENAME" +
//				"          , OBJNAME" +
//				"          , DB_OBJ_TYPE_CD" +
//				"          , COLUMNNAME" +
//				"          , ORDER_ID" +
//				"          , COLUMN_ID " +
//				"       FROM M04_COLUMN " +
//				"      ORDER BY DATABASENAME" +
//				"          , OBJNAME" +
//				"          , ORDER_ID";

		String sql = "SELECT UPPER(T3.DatabaseName) AS DatabaseName"
                   + "     , UPPER(T2.ObjName) AS ObjName"
                   + "     , T2.DB_OBJ_Type_CD AS DB_Obj_Type_Cd"
                   + "     , UPPER(T1.COLUMNNAME) AS COLUMNNAME"
                   + "     , T1.ORDER_ID AS ORDER_ID"
                   + "     , COALESCE(T1.Column_Is_Const_Cd, 'N') AS IS_CONST_CD"
                   + "     , T1.COLUMN_ID AS COLUMN_ID"
                   + "  FROM M04_TD_Column T1"
                   + "  LEFT JOIN M04_TD_Object T2"
                   + "    ON T1.Obj_ID = T2.Obj_ID"
                   + "   AND T2.Start_Dt <= current_timestamp(0)"
                   + "   AND T2.End_Dt > current_timestamp(0)"
                   + "  LEFT JOIN M04_TD_DB T3"
                   + "    ON T2.DB_ID = T3.DB_ID"
                   + "   AND T3.Start_Dt <= current_timestamp(0)"
                   + "   AND T3.End_Dt > current_timestamp(0)"
                   + " WHERE T1.Start_Dt <= current_timestamp(0)"
                   + "   AND T1.End_Dt > current_timestamp(0)"
                   + " ORDER BY DatabaseName, ObjName, Order_ID";
		
		
		
		HashMap colMap = new HashMap(RelaParser.HASHMAP_CAPACITY);
		List cols = null;
		try {
			DBQueryEngine engine = DBUtil.getEngine();
			cols = engine.getResultList(sql);
		} catch (QueryException e) {
			e.printStackTrace();
		}
		if (cols != null) {
			for (Iterator iterator = cols.iterator(); iterator.hasNext();) {
				Map col = (Map) iterator.next();
				String key = col.get("DATABASENAME") + postfix + "." + col.get("OBJNAME");

        if("CV_PVIEW.T99_STD_CDE_MAP_INFO".equals(key)) {
          System.out.println("Bingo");
        }

				List tableCols = (List) colMap.get(key);
				if (tableCols == null) {
					tableCols = new ArrayList();
					colMap.put(key, tableCols);
				}
				tableCols.add(col);
			}
		}

		return colMap;
	}

	private HashMap getTableMap() {
		String sql = "SELECT UPPER(A2.DatabaseName) AS DATABASENAME"
                +"      ,UPPER(A1.ObjName) AS OBJNAME"
                +"      ,UPPER(A1.DB_Obj_Type_Cd) AS DB_OBJ_TYPE_CD"
                +"      ,UPPER(A1.Obj_ID) AS OBJ_ID"
                +"  FROM M04_TD_OBJECT A1"
                +"  LEFT JOIN M04_TD_DB A2"
                +"    ON A1.DB_ID = A2.DB_ID"
                +"   AND A2.START_DT <= CURRENT_TIMESTAMP(0)"
                +"   AND A2.END_DT > CURRENT_TIMESTAMP(0)"
                +" WHERE A1.START_DT <= CURRENT_TIMESTAMP(0)"
                + "  AND A1.END_DT > CURRENT_TIMESTAMP(0);";

//		String sql = "SELECT " + "UPPER(DATABASENAME) AS DATABASENAME, " + "UPPER(OBJNAME) AS OBJNAME, "
//					+ "UPPER(DB_OBJ_TYPE_CD) AS DB_OBJ_TYPE_CD, " + "UPPER(OBJ_ID) AS OBJ_ID " + "FROM M04_OBJ;";

		HashMap tableMap = new HashMap(RelaParser.HASHMAP_CAPACITY);
		List tables = null;
		try {
			DBQueryEngine engine = DBUtil.getEngine();
			tables = engine.getResultList(sql);
		} catch (QueryException e) {
			e.printStackTrace();
		}
		if (tables != null) {
			System.out.println("Tables is " + tables.size());
			for (Iterator iterator = tables.iterator(); iterator.hasNext();) {
				Map table = (Map) iterator.next();
				String key = table.get("DATABASENAME") + postfix + "." + table.get("OBJNAME");
				tableMap.put(key, table.get("OBJ_ID"));
			}
		}

		return tableMap;
	}

	private Vector readTemplate() {
		
		List params = null;
		try {
			DBQueryEngine engine = DBUtil.getEngine();
			params = engine.getResultList(paramSql);
		} catch (QueryException e) {
			e.printStackTrace();
		}
		Vector templates = new Vector();
		if (params != null) {
			for (int i = 0; i < params.size(); i++) {
				Map param = (Map) params.get(i);
				templates.add(param.get("PARAM"));
			}
		}
		return templates;
	}

}
