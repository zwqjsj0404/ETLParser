package com.teradata.mds.etlparser.viewparse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.teradata.db.DBQueryEngine;
import com.teradata.mds.etlparser.DBUtil;
import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.relation.RelationDataWriter;
import com.teradata.mds.etlparser.relation.SQLParseResult;
import com.teradata.tap.system.query.QueryException;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

public class ViewParserLoader implements Runnable {
	private int threads = 0;
	private int sqlQueueSize = 0;
	private String targetpath;
	private boolean insertDB;
	private String postfix;
	private String dbPostfix;

	public ViewParserLoader(String targetpath, int threads, int sqlQueueSize, boolean insertDB, String postfix
			, String dbPostfix) {
		this.targetpath = targetpath;
		this.threads = threads;
		this.sqlQueueSize = sqlQueueSize;
		this.insertDB = insertDB;
		this.postfix = postfix;
		this.dbPostfix = dbPostfix;
	}

	public void run() {
		Map columnMap = getColumnMap();
		Map tableMap = getTableMap();
		Map scriptMap = getScriptMap();
		List views = getViews();
		int count = views.size();
		RelaParser.setTotalFiles(count);

		BlockingQueue viewQueue = new ArrayBlockingQueue(50);
		BlockingQueue sqlQueue = new ArrayBlockingQueue(sqlQueueSize);

		// 启动处理线程
		List parsers = new ArrayList();
		for (int i = 0; i < threads; i++) {
			Thread parser = new Thread(new ViewParseThread(viewQueue, sqlQueue, columnMap, tableMap, scriptMap), "View Parser");
			parser.start();
			parsers.add(parser);
		}

		// 启动写入线程
		Thread writer = new Thread(new RelationDataWriter(this.targetpath, "ViewParse", sqlQueue, this.insertDB), "View Parser Writer");
		writer.start();

		// 向队列写入View数据
		try {
			for (int i = 0; i < views.size(); i++) {
				Map view = (Map) views.get(i);
				viewQueue.put(view);
			}
			
			for(int i=0; i<parsers.size(); i++) {
				viewQueue.put(RelaParser.FINISH_STRING);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// 等待所有解析线程结束
		try {
			for (int i = 0; i < parsers.size(); i++) {
				Thread parser = (Thread) parsers.get(i);
				parser.join();
			}
			//写入结束标志
			sqlQueue.put(new SQLParseResult(RelaParser.FINISH_STRING, null));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// 等待写入线程结束
		try {
			writer.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return;
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
	
	/**
	 * 取得所有View信息
	 * 
	 * @return
	 */
	private List getViews() {
		String sql = "SELECT " + "REQUESTTEXT AS VIEWSQL," + "DATABASENAME || '" + postfix + "' AS DB, " + "OBJ_ID AS OBJ_ID "
				+ ", OBJNAME AS VIEWNAME " + "FROM MV04_VIEW ORDER BY 2, 4;";

		List views = new ArrayList();
		try {
			DBQueryEngine engine = DBUtil.getEngine();
			views = engine.getResultList(sql);
			if (views == null) {
				views = new ArrayList();
			}
		} catch (QueryException e) {
			e.printStackTrace();
		}
		return views;
	}

	/**
	 * 从M04_Obj视图中取得所有表信息
	 * 
	 * @return "库名.表名"作为主键的Map
	 */
	private Map getTableMap() {
//		String sql = "SELECT " + "UPPER(DATABASENAME|| '" + postfix + "')  AS DATABASENAME" + ", UPPER(OBJNAME) AS OBJNAME"
//				+ ", UPPER(DB_OBJ_TYPE_CD) AS DB_OBJ_TYPE_CD" + ", UPPER(OBJ_ID) AS OBJ_ID " + "FROM M04_OBJ;";
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

		HashMap tableMap = new HashMap(RelaParser.HASHMAP_CAPACITY);
		try {
			DBQueryEngine engine = DBUtil.getEngine();
			List tables = engine.getResultList(sql);
			if (tables != null) {
				for (Iterator iterator = tables.iterator(); iterator.hasNext();) {
					Map table = (Map) iterator.next();
					String key = table.get("DATABASENAME") + dbPostfix + "." + table.get("OBJNAME");
					tableMap.put(key, table.get("OBJ_ID"));
				}
			}
		} catch (QueryException e) {
			e.printStackTrace();
		}

		return tableMap;
	}

	/**
	 * 从M04_Column视图中取得所有列信息
	 * 
	 * @return "库名.表名"作为主键的Map
	 */
	private Map getColumnMap() {
//		String sql = "SELECT " + "DATABASENAME || '"+ postfix + "'" 
//		           + ", OBJNAME" 
//		           + ", DB_OBJ_TYPE_CD" 
//		           + ", COLUMNNAME" 
//		           + ", ORDER_ID"
//				   + ", COLUMN_ID "
//				   + " FROM M04_COLUMN " + "ORDER BY DATABASENAME, OBJNAME, ORDER_ID";

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
		try {
			DBQueryEngine engine = DBUtil.getEngine();
			List cols = engine.getResultList(sql);
			if (cols != null) {
				for (Iterator iterator = cols.iterator(); iterator.hasNext();) {
					Map col = (Map) iterator.next();
					String key = col.get("DATABASENAME") + dbPostfix + "." + col.get("OBJNAME");
					List tableCols = (List) colMap.get(key);
					if (tableCols == null) {
						tableCols = new ArrayList();
						colMap.put(key, tableCols);
					}
					tableCols.add(col);
				}
			}
		} catch (QueryException e) {
			e.printStackTrace();
		}

		return colMap;
	}

}
