package com.teradata.sqlparser.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.teradata.constants.MetaRelationTypeConstants;
import com.teradata.db.DBQueryEngine;
import com.teradata.engine.mds.MDSDbVO;
import com.teradata.engine.mds.exception.MDSException;
import com.teradata.mds.etlparser.relation.DataRelaTableVO;
import com.teradata.sqlparser.util.SQLExtractUtil;
import com.teradata.sqlparser.interpret.FromClause;
import com.teradata.sqlparser.interpret.FromTableDef;
import com.teradata.sqlparser.interpret.SearchExpression;
import com.teradata.sqlparser.interpret.TableSelectExpression;
import com.teradata.sqlparser.node.Expression;
import com.teradata.sqlparser.node.StatementTree;
import com.teradata.tap.system.query.QueryException;
import com.teradata.tap.system.util.PropertiesLoader;

/**
 * A class to parse sql statement, extract table level relations, then store those relations into table
 * TAP_C_META_UNIT_RELATION.
 * 
 */

public class tableRelationParser {

	private DBQueryEngine dLocal = null;
	private HashMap tabMap = null;
	private HashMap scriptMap = null;
	private ArrayList insertArray = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中的临时表信息。HashMap结构详细见parserCreateTalbe（）方法：
	 * 
	 * "table_name" name of the created table. Type: String "volatile" whether a volatile table. Type: Boolean
	 * "sqlIndex" sql index of the created table. Type: int "as_table" table name of the table created from。 Type：String
	 * "as_clause" subquery of the table created from。 Type: StatementTree "sourceTables" source tables of the created
	 * table。Type：ArrayList
	 **/
	private ArrayList volatileTableList = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中创建的非临时表信息。HashMap结构详细见parserCreateTalbe（）方法
	 **/
	private ArrayList commonTableList = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中的临时表的源表信息。HashMap结构详细见parserInsert（）方法：
	 * 
	 * "sqlIndex"，使用sqlIndex为了解决script脚本中有多个create语句使用同一个表名的情形。
	 * 
	 * 用sqlIndex来记录现在执行到哪个sql，同时将create table对应的sql的index值保存起来，选择两者距离最近的一个create table语句。 Type：int "table_name" table
	 * name of the volatile talbe. Type: String "sourceTableList" source tables of the volatile table. Type: ArrayList
	 **/
	private ArrayList volatileTableSourceList = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中的common表的源信息。HashMap结构详细见parserInsert（）方法。
	 **/
	private ArrayList commonTableSourceList = new ArrayList();

	private static Logger logger = Logger.getLogger(SqlParser.class);

	/**
	 * 使用sqlIndex为了解决script脚本中有多个create语句使用同一个表名的情形。
	 * 
	 * 用sqlIndex来记录现在执行到哪个sql，同时将create table对应的sql的index值保存起来
	 * 
	 * 选择两者距离最近的一个create table语句。 Type：int
	 **/
	private int sqlIndex = 0;

	// 用来存储传入的需解析的sql个数
	private int sqlNum = 0;

	// 用来存储sql个数阈值
	private int sql_num_limit = 0;

	// 记录中间关系的数目
	private int temp_relation_num = 0;

	private String tempDB = "";

	public tableRelationParser(DBQueryEngine dLocal, String tempDB, HashMap tabMap, HashMap scriptMap) throws MDSException {
		this.dLocal = dLocal;
		this.tabMap = tabMap;
		this.scriptMap = scriptMap;
		if (!tempDB.equals("")) {
			this.tempDB = tempDB + ".";
		}
	}

	public tableRelationParser(String tempDB, HashMap tabMap, HashMap scriptMap) {
		this.tabMap = tabMap;
		this.scriptMap = scriptMap;
		if (!tempDB.equals("")) {
			this.tempDB = tempDB + ".";
		}
	}

	/**
	 * get object unit info by 2 columns: dbname, ObjName
	 * 
	 * @param sysName
	 *            dbname
	 * @param ObjName
	 * @return
	 * @throws MDSException
	 */
	public MDSDbVO getUnitObject(String DatabaseName, String ObjName) throws MDSException {
		if(DatabaseName != null) DatabaseName = DatabaseName.trim();
		if(ObjName != null) ObjName = ObjName.trim();
		
		MDSDbVO mdsDb = new MDSDbVO();
		String globalID = null;
		String key = DatabaseName.toUpperCase() + "." + ObjName.toUpperCase();
		globalID = (String) tabMap.get(key);
		if (globalID == null || globalID == "") {
			if (DatabaseName == null || DatabaseName.trim().length() == 0)
				throw new MDSException("table : " + ObjName + " without database reference!");
			else
				throw new MDSException("table : " + DatabaseName + "." + ObjName + " is not loaded!");
		} else {
			String sysDesc = DatabaseName;
			String tableDesc = ObjName;
			mdsDb.setMETA_GLOBAL_ID(globalID);
			mdsDb.setSYS_DESC(sysDesc);
			mdsDb.setTABLE_DESC(tableDesc);
		}
		return mdsDb;
	}

	/**
	 * parse a delete statement and return void(We don't care about deleting statement);
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * @return
	 */
	public void parseDelete(StatementTree sTree) {

	}

	/**
	 * parse a update statement and return void(We don't care about updating statement);
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * @return
	 */
	public void parseUpdate(StatementTree sTree) {

	}

	/**
	 * parse a alter statement and return updated table info; here, we focus on tables which are created in script
	 * logs(stored in commonTableList).
	 * 
	 * We can get the entire tree info from its map(Type: HashMap). Because this is a "alter table" statement, it points
	 * to 'com.teradata.sqlparser.interpret.AlterTable'. The map's stucture: "table_name" the altered table name. Type:
	 * String "alter_action" alter action on the table. Here, wu focus on the following actions: "ADD", "DROP", "RENAME"
	 * Type: AlterTableAction
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * @return
	 */
	public void parseAlterTable(StatementTree sTree) {

	}

	/**
	 * parse a select statement and return void(We don't care about selecting statement);
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * 
	 *            We can get the entire tree info from its map(Type: HashMap). Because this is a "Select" statement, it
	 *            points to 'com.teradata.sqlparser.interpret.Select'. The map's stucture: "table_expression" table
	 *            expression of the selected table. Type: TableSelectExpression; "order_by" order by columuns of the
	 *            selected table. Type: ArrayList;
	 * 
	 * @return
	 */
	public void parseSelect(StatementTree sTree) {

	}

	public boolean ifBeyondLimit() {
		boolean beyond = false;
		if (temp_relation_num > 150000)
			beyond = true;
		return beyond;
	}

	/**
	 * parse a create table statement and return table info("table_name",
	 * "volatile","as_table","as_clause","columnNameList");
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * 
	 *            We can get the entire tree info from its map(Type: HashMap). Because this is a "create table"
	 *            statement, it points to 'com.teradata.sqlparser.interpret.CreateTable'. The map's stucture:
	 *            "table_name" name of the created table "temporary" if a temporary table "volatile" if a volatile table
	 *            "multiset" if a multiset table "column_list" columns of the created table "constraint_list" constrait
	 *            info of the created table "index_list" index info of the created table "as_table" table name created
	 *            from "as_clause" sub query in the created table
	 * 
	 * @return
	 * @throws MDSException
	 * @throws MDSException
	 */
	public HashMap parseCreateTable(StatementTree sTree) throws MDSException {
		HashMap tableInfo = new HashMap();
		ArrayList tablesInCreate = new ArrayList();
		HashMap map = sTree.map;
		// table name
		String table_name = (String) map.get("table_name");
		// if a volatile table
		Boolean ifVolatile = (Boolean) map.get("volatile");
		// table name of the table created from
		String sourceTable = (String) map.get("as_table");
		// subquery of the table created from
		StatementTree select = (StatementTree) map.get("as_clause");

		if (select != null) {
			HashMap asmap = select.map;
			TableSelectExpression selExp = (TableSelectExpression) asmap.get("table_expression");
			// table names in as clause
			tablesInCreate = getAllTables(selExp);

			ArrayList sourceTables = new ArrayList();
			ArrayList resultTableList = new ArrayList();

			// 调用getSourceTableList函数获得select中相关表的源表
			for (int i = 0; i < tablesInCreate.size(); i++) {
				String exp = tablesInCreate.get(i) + "==>" + table_name;
				sourceTables = getSourceTableList((String) tablesInCreate.get(i));
				temp_relation_num = temp_relation_num + sourceTables.size();
				if (ifBeyondLimit()) {
					return null;
				}
				for (int j = 0; j < sourceTables.size(); j++) {
					HashMap table = (HashMap) sourceTables.get(j);
					HashMap resultTable = new HashMap();
					if (table.get("sourceTable") == "") {
						resultTable.put("targetTable", table_name);
						resultTable.put("sourceTable", (String) table.get("targetTable"));
						// resultTable.put("expression", getSubComm(exp));
						resultTableList.add(resultTable);
					} else {
						resultTable.put("targetTable", table_name);
						resultTable.put("sourceTable", (String) table.get("sourceTable"));
						/*
						 * String commentID = (String) table.get("expression"); String relationComm = commentID; if (
						 * sqlNum > sql_num_limit ) relationComm = getComment(commentID); if ( blankComment )
						 * resultTable.put("expression",
						 * "Relation transformation is too complex, please refer to the script for detailed info...");
						 * else resultTable.put("expression", getSubComm(relationComm + " ==> " + table_name));
						 */
						resultTableList.add(resultTable);
					}
				}
			}

			HashMap volatileTableSource = new HashMap();
			HashMap commonTableSource = new HashMap();
			// add all the valatile table resources into volatileTableSourceList
			if (ifVolatile.booleanValue()) {
				volatileTableSource.put("sqlIndex", new Integer(sqlIndex));
				volatileTableSource.put("table_name", table_name);
				volatileTableSource.put("sourceTableList", new HashSet(resultTableList));
				volatileTableSourceList.add(volatileTableSource);
			} else {
				commonTableSource.put("sqlIndex", new Integer(sqlIndex));
				commonTableSource.put("table_name", table_name);
				commonTableSource.put("sourceTableList", new HashSet(resultTableList));
				commonTableSourceList.add(commonTableSource);
			}
		}

		tableInfo.put("table_name", table_name);
		tableInfo.put("volatile", ifVolatile);
		tableInfo.put("as_table", sourceTable);
		tableInfo.put("as_clause", select);
		tableInfo.put("sqlIndex", new Integer(sqlIndex));
		tableInfo.put("sourceTables", tablesInCreate);

		return tableInfo;
	}

	/**
	 * get all tables in a select statement;
	 * 
	 * @param selExp
	 *            select statement.
	 * @return
	 */
	public ArrayList getAllTables(TableSelectExpression selExp) {
		ArrayList sourceTableList = new ArrayList();

		// from clause in select statement
		FromClause fromclause = selExp.from_clause;
		ArrayList def_list = fromclause.def_list;
		for (int i = 0; i < def_list.size(); i++) {
			FromTableDef table_def = (FromTableDef) def_list.get(i);
			String tableName = table_def.getName();
			// if the table is a subquery table
			if (table_def.isSubQueryTable()) {
				TableSelectExpression sub_query = table_def.getTableSelectExpression();
				ArrayList tablesInSubQuery = getAllTables(sub_query);
				for (int j = 0; j < tablesInSubQuery.size(); j++) {
					sourceTableList.add(tablesInSubQuery.get(j));
				}
			} else {
				sourceTableList.add(tableName);
			}
		}

		// where clause in select statement
		SearchExpression where = (SearchExpression) selExp.where_clause;
		Expression whereExp = where.getFromExpression();
		if (whereExp != null) {
			for (int i = 0; i < whereExp.allElements().size(); i++) {
				Object element = whereExp.allElements().get(i);
				// if existing sub query in where clause
				if (element.getClass().getName().equals("com.teradata.sqlparser.interpret.TableSelectExpression")) {
					TableSelectExpression tablesel = (TableSelectExpression) element;
					ArrayList tablesInWhere = getAllTables(tablesel);
					for (int j = 0; j < tablesInWhere.size(); j++) {
						sourceTableList.add(tablesInWhere.get(j));
					}
				}
			}
		}

		// 处理union语句
		while (selExp.composite_function != -1) {
			selExp = selExp.next_composite;
			ArrayList tablesInUnion = getAllTables(selExp);
			for (int j = 0; j < tablesInUnion.size(); j++) {
				sourceTableList.add(tablesInUnion.get(j));
			}
		}
		return sourceTableList;
	}

	/**
	 * parse insert statement;
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * 
	 *            We can get the entire tree info from its map(Type: HashMap). Because this is a "Insert" statement, it
	 *            points to 'com.teradata.sqlparser.interpret.Insert'. The map's stucture: "table_name" the inserted
	 *            table name. Type: String; "col_list" columns in the inserted statement. Type: ArrayList; "data_list"
	 *            values of the inserted statement. Type: ArrayList; "select" select clause of the inserted statement.
	 *            Type: StatementTree; "type" whether the inserted value is 'from_values' or 'from_select'. Type: String
	 * 
	 * @return
	 * @throws MDSException
	 * @throws MDSException
	 */
	public ArrayList parseInsert(StatementTree sTree) throws MDSException {
		ArrayList sourceTables = new ArrayList();
		ArrayList resultTableList = new ArrayList();
		HashMap map = sTree.map;
		String tableNameWithDot = (String) map.get("table_name"); // target table "database.table"

		StatementTree select = (StatementTree) map.get("select"); // query in the insert statement
		if (select != null) {
			HashMap selmap = select.map;
			TableSelectExpression selExp = (TableSelectExpression) selmap.get("table_expression");
			// get all related tables
			ArrayList tables = new ArrayList();
			tables = getAllTables(selExp);

			// 调用getSourceTableList函数获得select中相关表的源表
			for (int i = 0; i < tables.size(); i++) {
				// String exp = tables.get(i) + "==>" + tableNameWithDot;
				sourceTables = getSourceTableList((String) tables.get(i));
				temp_relation_num = temp_relation_num + sourceTables.size();
				if (ifBeyondLimit()) {
					return null;
				}
				for (int j = 0; j < sourceTables.size(); j++) {
					HashMap sourceTable = (HashMap) sourceTables.get(j);
					HashMap resultTable = new HashMap();
					if (sourceTable.get("sourceTable") == "") {
						resultTable.put("targetTable", tableNameWithDot);
						resultTable.put("sourceTable", (String) sourceTable.get("targetTable"));
						// resultTable.put("expression", getSubComm(exp));
						resultTableList.add(resultTable);
					} else {
						resultTable.put("targetTable", tableNameWithDot);
						resultTable.put("sourceTable", (String) sourceTable.get("sourceTable"));
						/*
						 * String commentID = (String) sourceTable.get("expression"); String relationComm = commentID;
						 * if ( sqlNum > sql_num_limit ) relationComm = getComment(commentID); if ( blankComment )
						 * resultTable.put("expression",
						 * "Relation transformation is too complex, please refer to the script for detailed info...");
						 * else resultTable.put("expression", getSubComm(relationComm + " ==> " + tableNameWithDot));
						 */
						resultTableList.add(resultTable);
					}
				}
			}
		}

		HashMap volatileTableSource = new HashMap();
		HashMap commonTableSource = new HashMap();
		int index = -1;
		index = volatileIndex(tableNameWithDot);
		// add all the valatile table resources into volatileTableSourceList
		if (index >= 0) {
			volatileTableSource.put("sqlIndex", new Integer(index));
			volatileTableSource.put("table_name", tableNameWithDot);
			volatileTableSource.put("sourceTableList", new HashSet(resultTableList));
			volatileTableSourceList.add(volatileTableSource);
			resultTableList = new ArrayList();
			return new ArrayList(new HashSet(resultTableList));
		}

		else
			index = commonIndex(tableNameWithDot);

		if (index >= 0) {
			commonTableSource.put("sqlIndex", new Integer(index));
			commonTableSource.put("table_name", tableNameWithDot);
			commonTableSource.put("sourceTableList", new HashSet(resultTableList));
			commonTableSourceList.add(commonTableSource);
			resultTableList = new ArrayList();
			return new ArrayList(new HashSet(resultTableList));
		}
		return new ArrayList(new HashSet(resultTableList));
	}

	/**
	 * get source table list according to table name;
	 * 
	 * @param table
	 *            name,expression string.
	 * @return
	 */
	public ArrayList getSourceTableList(String tableName) {
		ArrayList sourceTableList = new ArrayList();
		HashSet tableSet = new HashSet();
		// whether the table is a volatile table
		int index = -1;
		index = volatileIndex(tableName);
		// is a volatile table
		if (index >= 0) {
			for (int i = 0; i < volatileTableSourceList.size(); i++) {
				HashMap volatileTable = (HashMap) volatileTableSourceList.get(i);
				Integer volatileIndex = (Integer) volatileTable.get("sqlIndex");
				if (index == volatileIndex.intValue()) {
					tableSet = (HashSet) volatileTable.get("sourceTableList");
					ArrayList tableList = new ArrayList(tableSet);

					for (int j = 0; j < tableList.size(); j++) {
						HashMap table = (HashMap) tableList.get(j);
						String sTable_name = (String) table.get("sourceTable");
						// String expressionExp = (String) table.get("expression");
						HashMap sTable = new HashMap();
						sTable.put("sourceTable", sTable_name);
						sTable.put("targetTable", tableName);
						// sTable.put("expression",expressionExp);
						sourceTableList.add(sTable);
					}
				}
			}
		} else {
			index = commonIndex(tableName);
			if (index >= 0) {
				for (int i = 0; i < commonTableSourceList.size(); i++) {
					HashMap commonTable = (HashMap) commonTableSourceList.get(i);
					Integer commonIndex = (Integer) commonTable.get("sqlIndex");
					if (index == commonIndex.intValue()) {
						tableSet = (HashSet) commonTable.get("sourceTableList");
						ArrayList tableList = new ArrayList(tableSet);

						for (int j = 0; j < tableList.size(); j++) {
							HashMap table = (HashMap) tableList.get(j);
							String sTable_name = (String) table.get("sourceTable");
							// String expressionExp = (String) table.get("expression");
							HashMap sTable = new HashMap();
							sTable.put("sourceTable", sTable_name);
							sTable.put("targetTable", tableName);
							// sTable.put("expression",expressionExp);
							sourceTableList.add(sTable);
						}
					}
				}
			} else {
				HashMap sourceTable = new HashMap();
				sourceTable.put("sourceTable", "");
				sourceTable.put("targetTable", tableName);
				// sourceTable.put("expression",expString);
				sourceTableList.add(sourceTable);
			}
		}
		return new ArrayList(new HashSet(sourceTableList));
	}

	/**
	 * decide if a table is a volatile table; if not volatile, return -1; else return the index of this sql
	 * 
	 * @param tableName
	 *            ,sqlIndex.
	 * @return
	 */
	public int volatileIndex(String tableName) {
		int index = -1;
		int distance = 100000; // 当脚本中同一名称的表有多个create语句时，判断使用哪个create语句
		for (int i = 0; i < volatileTableList.size(); i++) {
			HashMap volatileTable = (HashMap) volatileTableList.get(i);
			if (tableName.equalsIgnoreCase((String) volatileTable.get("table_name"))) {
				Integer createIndex = (Integer) volatileTable.get("sqlIndex");
				if ((sqlIndex - createIndex.intValue()) <= distance) {
					distance = sqlIndex - createIndex.intValue();
					index = createIndex.intValue();
				}
			}
		}
		return index;
	}

	/**
	 * decide if a table is a common table created in a script; if not volatile, return -1; else return the index of
	 * this sql
	 * 
	 * @param tableName
	 *            ,sqlIndex.
	 * @return
	 */
	public int commonIndex(String tableName) {
		int index = -1;
		int distance = 100000; // 当脚本中同一名称的表有多个create语句时，判断使用哪个create语句
		for (int i = 0; i < commonTableList.size(); i++) {
			HashMap commonTable = (HashMap) commonTableList.get(i);
			if (tableName.equalsIgnoreCase((String) commonTable.get("table_name"))) {
				Integer createIndex = (Integer) commonTable.get("sqlIndex");
				if ((sqlIndex - createIndex.intValue()) <= distance) {
					distance = sqlIndex - createIndex.intValue();
					index = createIndex.intValue();
				}
			}
		}
		return index;
	}

	/**
	 * parse Create view statement;
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * @return
	 * @throws MDSException
	 */
	public ArrayList parseCreateView(StatementTree sTree, String sysName) throws MDSException {

		ArrayList sourceTables = new ArrayList();
		ArrayList resultTableList = new ArrayList();
		HashMap map = sTree.map;
		// view name
		String viewNameWithDot = (String) map.get("view_name");
		String[] temp = viewNameWithDot.split("\\.");
		if (temp.length == 1)// 只有view的名称，不带库名前缀
		{
			viewNameWithDot = sysName + "." + viewNameWithDot;
		}

		// view source table definition
		TableSelectExpression selExp = (TableSelectExpression) map.get("select_expression");

		// get all related tables
		ArrayList tables = new ArrayList();
		tables = getAllTables(selExp);

		// 调用getSourceTableList函数获得select中相关表的源表
		for (int i = 0; i < tables.size(); i++) {
			// String exp = tables.get(i) + "==>" + viewNameWithDot;
			sourceTables = getSourceTableList((String) tables.get(i));
			temp_relation_num = temp_relation_num + sourceTables.size();
			if (ifBeyondLimit()) {
				return null;
			}
			for (int j = 0; j < sourceTables.size(); j++) {
				HashMap sourceTable = (HashMap) sourceTables.get(j);
				HashMap resultTable = new HashMap();
				if (sourceTable.get("sourceTable") == "") {
					resultTable.put("targetTable", viewNameWithDot);
					resultTable.put("sourceTable", (String) sourceTable.get("targetTable"));
					// resultTable.put("expression", getSubComm(exp));
					resultTableList.add(resultTable);
				} else {
					resultTable.put("targetTable", viewNameWithDot);
					resultTable.put("sourceTable", (String) sourceTable.get("sourceTable"));
					/*
					 * String commentID = (String) sourceTable.get("expression"); String relationComm = commentID; if (
					 * sqlNum > sql_num_limit ) relationComm = getComment(commentID); if ( blankComment )
					 * resultTable.put("expression",
					 * "Relation transformation is too complex, please refer to the script for detailed info..."); else
					 * resultTable.put("expression", getSubComm(relationComm + " ==> " + viewNameWithDot));
					 */
					resultTableList.add(resultTable);
				}
			}
		}

		return new ArrayList(new HashSet(resultTableList));
	}

	/**
	 * insert the relationship of two columns into a temp table VT_TAP_C_META_UNIT_RELATION;
	 * 
	 * @param
	 * @return
	 */
	public void addRelation(String Src_Obj_ID
			              , String Tgt_Obj_ID
			              , String Data_Rela_Modify
			              , String Data_Rela_Cd
			              ,	String Auto_Type_Cd
			              , String Data_Rela_Src_ID
			              , String Data_Rela_Src_Name) {
		// String sql = "insert into " + this.tempDB+ "MT02_DATA_RELA_T " +
		// " values ('" + Src_Obj_ID +
		// "','" + Tgt_Obj_ID +
		// "','" + Data_Rela_Modify +
		// "'," + Data_Rela_Cd +
		// "," + Auto_Type_Cd +
		// ",'" + Data_Rela_Src_ID +
		// "')";

		//modified by MarkDong
		// FIXME: 表字段Data_Rela_Scr_ID和Data_Rela_Src_Name有些混乱
		int data_rela_cd = Integer.parseInt(Data_Rela_Cd);
		int auto_type_cd = Integer.parseInt(Auto_Type_Cd);

		//FIXME:需要放开
		DataRelaTableVO relaTVO = new DataRelaTableVO(Src_Obj_ID
				                                    , Tgt_Obj_ID
				                                    , Data_Rela_Modify
				                                    , Data_Rela_Src_ID
				                                    , data_rela_cd
				                                    , auto_type_cd
				                                    , Data_Rela_Src_Name
				                                    , ""
				                                    , "");

		insertArray.add(relaTVO);
	}

	/**
	 * get object id by 2 columns: dbname, objname
	 * 
	 * @param sysName
	 *            dbname
	 * @param objName
	 * @return
	 * @throws MDSException
	 */
	public String getObjID(String sysName, String objName) throws MDSException {
		String objID = null;
		String key = sysName.toUpperCase() + "." + objName.toUpperCase();
		objID = (String) tabMap.get(key);

		if (objID == null || objID == "") {
			if (sysName == null || sysName.trim().length() == 0)
				throw new MDSException("table : " + objName + " without database reference!");
			else
				throw new MDSException("table : " + sysName + "." + objName
						+ " is not loaded or some columns in this table are not loaded!");
		} else
			return objID;
	}
	
	/**
	 * get script id by script name: Script_Name
	 * 
	 * @param Script_Name
	 * @return
	 * @throws MDSException
	 */
	public String getScriptID(String scriptName) throws MDSException {

		String scriptID = null;
		String key = scriptName.toUpperCase();
		scriptID = (String) scriptMap.get(key);
		if (scriptID == null || scriptID == "") {
			scriptID = "";
			//logger.warn("script : " + scriptName + " is not loaded !");
		}
		return scriptID;
	}
	
	/**
	 * transfer a string into a stream.
	 * 
	 * @param content
	 *            the string to be transfered.
	 * @return
	 */
	public java.io.InputStream getStream(String content) {
		try {
			java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(content.getBytes());
			return bais;
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			return null;
		}
	}

	/**
	 * read a sql log file to db.
	 * 
	 * @param sqlList
	 *            sqls extracted from a file. fileName the file to be read.
	 * 
	 */
	public ArrayList bteqBridge(List sqlList, String fileName) throws MDSException {

		sqlNum = sqlList.size();

		// PropertiesLoader bootProps;
		// try {
		// bootProps = PropertiesLoader.getLoader("tap.properties");
		// String limit = bootProps.getProperty("SQL_NUM_LIMIT");
		// if ( limit != null && limit.length() >0 )
		// sql_num_limit = Integer.parseInt(limit);
		// else
		// sql_num_limit = 100000;
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		sql_num_limit = 100000; // modified by MarkDong

		int pos = fileName.indexOf(".log");
		if (pos > 0)
			fileName = fileName.substring(0, pos);
		String scriptID = getScriptID(fileName);
		
		try {
			for (int i = 0; i < sqlList.size(); i++) {
				ArrayList sourceTables = new ArrayList();
				sqlIndex = i;
				String sql = (String) sqlList.get(i);
				SQL parser = new SQL(getStream(sql));
				StatementTree statement;
				statement = parser.Statement();
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.Select")) {
					parseSelect(statement);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.Delete")) {
					parseDelete(statement);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.Insert")) {
					sourceTables = parseInsert(statement);
					if (sourceTables == null) {
						return null;
					}
					for (int t = 0; t < sourceTables.size(); t++) {
						HashMap resultTable = (HashMap) sourceTables.get(t);
						String targetTable = (String) resultTable.get("targetTable");
						String sourceTable = (String) resultTable.get("sourceTable");
						// System.out.println(targetTable + "   " + sourceTable + "   " + expression);

						String[] target = targetTable.split("\\.");
						String[] source = sourceTable.split("\\.");
						try {
							// target table info
							MDSDbVO targetUnit = getUnitObject(target[0], target[1]);
							String targetGlobalID = targetUnit.getMETA_GLOBAL_ID();
							// source table info
							MDSDbVO sourceUnit = getUnitObject(source[0], source[1]);
							String globalID = sourceUnit.getMETA_GLOBAL_ID();

							// relation type and comment
							/*
							 * String commentID = (String) resultTable.get("expression"); String relationComm =
							 * commentID; if ( sqlNum > sql_num_limit ) relationComm = getComment(commentID);
							 */
							String relationID = "13";

							addRelation(globalID
									  , targetGlobalID
									  , ""
									  , relationID
									  , "1"
									  , scriptID,fileName);

						} catch (MDSException e) {
							// logger.error(e.getMessage());
							throw new MDSException(e);
						} catch (Exception e) {
							// logger.error(e.getMessage());
							throw new MDSException("table : " + sourceTable + " or table: " + targetTable
									+ " without database reference!", e);
						}
					}
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.ViewManager")) {
					sourceTables = parseCreateView(statement, fileName);
					if (sourceTables == null) {
						return null;
					}
					for (int t = 0; t < sourceTables.size(); t++) {
						HashMap resultTable = (HashMap) sourceTables.get(t);
						String targetTable = (String) resultTable.get("targetTable");
						String sourceTable = (String) resultTable.get("sourceTable");
						String[] target = targetTable.split("\\.");
						String[] source = sourceTable.split("\\.");
						try {
							// target table info
							MDSDbVO targetUnit = getUnitObject(target[0], target[1]);
							String targetGlobalID = targetUnit.getMETA_GLOBAL_ID();
							// source table info
							MDSDbVO sourceUnit = getUnitObject(source[0], source[1]);
							String globalID = sourceUnit.getMETA_GLOBAL_ID();

							String relationID = "13";
							//String relSrcID = getObjID(fileName, target[1]);
							String relSrcID = getObjID(target[0], target[1]);	//edit by markdong
							addRelation(globalID, targetGlobalID, "", relationID, "1", relSrcID, targetTable);
						} catch (Exception e) {
							// logger.error(e.getMessage());
							throw new MDSException("table : " + source[0] + " OR table : " + target[0]
									+ " without database reference!", e);
						}
					}
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.CreateTable")) {
					// get all volitale tables
					HashMap tableInfo = parseCreateTable(statement);
					if (tableInfo == null) {
						return null;
					}
					Boolean ifVolitale = (Boolean) tableInfo.get("volatile");
					if (ifVolitale.booleanValue())
						volatileTableList.add(tableInfo);
					else
						commonTableList.add(tableInfo);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.UpdateTable")) {
					parseUpdate(statement);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.AlterTable"))
					parseAlterTable(statement);
				if (sourceTables != null)
					sourceTables.clear();
				parser = null;
				statement = null;
			}
		} catch (Throwable e) {
			throw new MDSException(fileName + " parse error", e);
		}
		return this.insertArray;
	}
}
