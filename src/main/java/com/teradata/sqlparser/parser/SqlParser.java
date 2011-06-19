package com.teradata.sqlparser.parser;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.teradata.db.DBQueryEngine;
import com.teradata.engine.mds.MDSDbVO;
import com.teradata.engine.mds.exception.MDSException;
import com.teradata.mds.etlparser.relation.DataRelaColumnVO;
import com.teradata.sqlparser.util.SQLExtractUtil;
import com.teradata.sqlparser.interpret.AlterTableAction;
import com.teradata.sqlparser.interpret.ColumnDef;
import com.teradata.sqlparser.interpret.FromClause;
import com.teradata.sqlparser.interpret.FromTableDef;
import com.teradata.sqlparser.interpret.SearchExpression;
import com.teradata.sqlparser.interpret.SelectColumn;
import com.teradata.sqlparser.interpret.TableSelectExpression;
import com.teradata.sqlparser.node.Expression;
import com.teradata.sqlparser.node.FunctionDef;
import com.teradata.sqlparser.node.JoinPart;
import com.teradata.sqlparser.node.JoiningSet;
import com.teradata.sqlparser.node.Operator;
import com.teradata.sqlparser.node.StatementTree;
import com.teradata.sqlparser.node.TArrayType;
import com.teradata.sqlparser.node.TObject;
import com.teradata.sqlparser.node.TType;
import com.teradata.sqlparser.node.TableName;
import com.teradata.sqlparser.node.Variable;
import com.teradata.tap.system.query.QueryException;
import com.teradata.tap.system.util.PropertiesLoader;

/**
 * A class to parse sql statement, extract column level relations, then store those relations into table
 * TAP_C_META_UNIT_RELATION.
 * 
 */

public class SqlParser {

	private DBQueryEngine dLocal = null;
	private HashMap colMap = null;
	private HashMap tabMap = null;
	private HashMap scriptMap = null;
	private List insertArray = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中的临时表信息。HashMap结构详细见parserCreateTalbe（）方法： "table_name" name of the created table. Type:
	 * String "volatile" whether a volatile table. Type: Boolean "sqlIndex" sql index of the created table. Type: int
	 * "column_list" columns of the created table. Type: ArrayList "as_table" table name of the table created from。
	 * Type：String "as_clause" subquery of the table created from。 Type: StatementTree "columnNameList" create语句中as
	 * table或as clause中的所有列名。Type：ArrayList
	 **/
	private List volatileTableList = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中的临时表每个列对应的源信息。HashMap结构详细见parserInsert（）方法：
	 * "sqlIndex"，使用sqlIndex为了解决script脚本中有多个create语句使用同一个表名的情形。 用sqlIndex来记录现在执行到哪个sql，同时将create
	 * table对应的sql的index值保存起来，选择两者距离最近的一个create table语句。 Type：int "table_name" table name of the volatile talbe. Type:
	 * String "sourceColumnList" source columns of the volatile table. Type: ArrayList
	 **/
	private List volatileTableSourceList = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中创建的非临时表信息。HashMap结构详细见parserCreateTalbe（）方法
	 **/
	private List commonTableList = new ArrayList();

	/**
	 * HashMap列表，用来存储log文件中的common表每个列对应的源信息。HashMap结构详细见parserInsert（）方法。
	 **/
	private List commonTableSourceList = new ArrayList();

	private static Logger logger = Logger.getLogger(SqlParser.class);

	/**
	 * 使用sqlIndex为了解决script脚本中有多个create语句使用同一个表名的情形。 用sqlIndex来记录现在执行到哪个sql，同时将create table对应的sql的index值保存起来
	 * 选择两者距离最近的一个create table语句。 Type：int
	 **/
	private int sqlIndex = 0;

	// 当解析视图时，用来存储缺省的数据库名
	private String fileName = "";

	// 用来存储传入的需解析的sql个数
	private int sqlNum = 0;

	// 用来存储sql个数阈值
	private int sql_num_limit = 500000;

	// comment是否置为空
	private static boolean blankComment = false;

	// 记录中间关系的数目
	private int temp_relation_num = 0;

	// public static List colTableRelations = new ArrayList();
	private String tempDB = "";

	public SqlParser(DBQueryEngine dLocal, String tempDB, HashMap colMap, HashMap tabMap, HashMap scriptMap)
			throws MDSException {
		this.dLocal = dLocal;
		this.colMap = colMap;
		this.tabMap = tabMap;
		this.scriptMap = scriptMap;
		if (!tempDB.equals("")) {
			this.tempDB = tempDB + ".";
		}
		createVolatileTable();
	}

	public SqlParser(String tempDB, HashMap colMap, HashMap tabMap, HashMap scriptMap) throws MDSException {
		this.colMap = colMap;
		this.tabMap = tabMap;
		this.scriptMap = scriptMap;
		if (!tempDB.equals("")) {
			this.tempDB = tempDB + ".";
		}
		// createVolatileTable();
	}

	/**
	 * get unit info by 3 columns: dbname, tablename, columnname
	 * 
	 * @param sysName
	 *            dbname
	 * @param tableName
	 * @param columnName
	 * @return
	 * @throws MDSException
	 */
	public MDSDbVO getUnitDB(String sysName, String tableName, String columnName) throws MDSException {
		if(sysName != null) sysName = sysName.trim();
		if(tableName != null) tableName = tableName.trim();
		if(columnName != null) columnName = columnName.trim();
		
		MDSDbVO mdsDb = new MDSDbVO();
		String globalID = null;
		if (sysName == null || sysName.trim().length() == 0)
			throw new MDSException("table : " + tableName + " without database reference!");
		else {
			String key = sysName.toUpperCase() + "." + tableName.toUpperCase();
			List ret = (List) colMap.get(key);
			if (ret == null)
				throw new MDSException(key + " is not loaded!");
			else {
				for (int i = 0; i < ret.size(); i++) {
					Map colInfo = (Map) ret.get(i);
					String colName = (String) colInfo.get("COLUMNNAME");
					globalID = (String) colInfo.get("COLUMN_ID");
					if (colName.equalsIgnoreCase(columnName)) {
						String sysDesc = sysName;
						String tableDesc = tableName;
						mdsDb.setMETA_GLOBAL_ID(globalID);
						mdsDb.setSYS_DESC(sysDesc);
						mdsDb.setTABLE_DESC(tableDesc);
						return mdsDb;
					}
				}
				if (globalID == null || globalID == "") {
					throw new MDSException("table : " + sysName + "." + tableName
							+ " is not loaded or some columns in this table are not loaded!");
				}
			}
		}
		return mdsDb;
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
		}
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
	 * create statement create a volatiole table to store relation comment
	 */
	public void createVolatileTable() throws MDSException {
		// 存放中间关系的可变临时表
		String sql = "DROP TABLE VT_RELATION_COMMENT;";
		try {
			dLocal.executeQuery(sql);
		} catch (QueryException e) {
		}
		try {
			sql = "CREATE MULTISET VOLATILE TABLE VT_RELATION_COMMENT(" + "commentID varchar(40) "
					+ ",commentDetail VARCHAR(10000)) " + "PRIMARY INDEX (commentID) " + "ON COMMIT PRESERVE ROWS; ";
			dLocal.executeQuery(sql);
		} catch (QueryException e) {
			throw new MDSException(e);
		}
	}

	/**
	 * This funciton is to rosolve the large size relation comment problem. Store relation commnet into table, and get
	 * it when needed
	 * 
	 * @throws MDSException
	 */
	public String insertComment(String comment) throws MDSException {
		String commentID = "";
		Date date = new Date();
		commentID = date.getYear() + "-" + date.getMonth() + "-" + date.getDay() + " " + date.getHours() + ":"
				+ date.getMinutes() + ":" + date.getSeconds() + System.currentTimeMillis();

		String commentDetail = comment.replaceAll("'", "''");
		if (commentDetail.length() >= 3500) {
			commentDetail = commentDetail.substring(0, 3400);
			int l = commentDetail.length();
			if (commentDetail.substring(l - 1).equals("'") && (!commentDetail.substring(l - 2).equals("''"))) {
				commentDetail = commentDetail.substring(0, l - 1);
			}
			commentDetail = commentDetail + "......";
		}
		try {

			String exeSql = "" + " INSERT INTO VT_RELATION_COMMENT (commentID,commentDetail) " + " VALUES ( '"
					+ commentID + "' , '" + commentDetail + "');";

			dLocal.executeUpdate(exeSql);
		} catch (QueryException e) {
			throw new MDSException(e);
		}
		// System.out.println("++++++"+commID);
		// System.out.println("------"+commentDetail);
		return commentID;
	}

	/**
	 * get comment detail from the volatile table according to comment id
	 * 
	 * @throws MDSException
	 */
	public String getComment(String commentID) throws MDSException {
		String commentDetail = "";
		commentID = commentID.replaceAll("'", "''");
		String sql = "select commentDetail from VT_RELATION_COMMENT " + "where commentID = '" + commentID + "';";

		// System.out.println("===="+sql);
		try {
			dLocal.executeQuery(sql);
			if (dLocal.next()) {
				commentDetail = dLocal.getString("commentDetail");
			}
		} catch (QueryException e) {
			throw new MDSException(e);
		}
		return commentDetail;
	}

	/**
	 * get sub comment, length <=500
	 * 
	 * @throws MDSException
	 */
	public String getSubComm(String comment) {
		// comment = comment.replaceAll("'","''");
		if (comment.length() >= 3500) {
			comment = comment.substring(0, 3400);
			int l = comment.length();
			if (comment.substring(l - 1).equals("'") && (!comment.substring(l - 2).equals("''"))) {
				comment = comment.substring(0, l - 1);
			}
			comment = comment + "......";
		}
		return comment;
	}

	/**
	 * parse a delete statement and return void(We don't care about deleting statement);
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * 
	 *            We can get the entire tree info from its map(Type: HashMap). Because this is a "delete" statement, it
	 *            points to 'com.teradata.sqlparser.interpret.Delete'. The map's stucture: "tableList" tables info to be
	 *            deleted. Type: ArrayList of HashMap ("table_name","correlation_name") "where_clause" where clause in
	 *            the deleted statement. Type: SearchExpression
	 * 
	 * @return
	 */
	public void parseDelete(StatementTree sTree) {

	}

	/**
	 * parse a update statement and return void(We don't care about updating statement);
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * 
	 *            We can get the entire tree info from its map(Type: HashMap). Because this is a "update table"
	 *            statement, it points to 'com.teradata.sqlparser.interpret.UpdateTable'. The map's stucture:
	 *            "table_name" the updated table name. Type: String "where_clause" where clause in the deleted
	 *            statement. Type: SearchExpression
	 * 
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
	 * @throws MDSException
	 */
	public HashMap parseAlterTable(StatementTree sTree) throws MDSException {

		HashMap tableInfo_new = new HashMap();
		HashMap map = sTree.map;
		// table name to be altered
		String table_name = (String) map.get("table_name");
		/*
		 * //alter table action of this alter statement AlterTableAction actions = (AlterTableAction)
		 * map.get("alter_actions"); //alter action name String actionName = actions.getAction(); //alter action
		 * parameters ArrayList parameters = actions.getElements();
		 */
		List actions = (ArrayList) map.get("alter_actions");

		String sysName = "";
		String tableName = table_name;
		String[] table_name_array = table_name.split("\\.");
		if (table_name_array.length == 2) {
			sysName = table_name_array[0].trim();
			tableName = table_name_array[1].trim();
		}
		try {
			// get table info of the altered table before the altering
			int commonIndex = commonIndex(table_name);
			HashMap tableInfo_old = new HashMap();
			for (int i = 0; i < commonTableList.size(); i++) {
				tableInfo_old = (HashMap) commonTableList.get(i);
				Integer index = (Integer) tableInfo_old.get("sqlIndex");
				if (index.intValue() == commonIndex) {
					break;
				}
			}

			tableInfo_new = tableInfo_old;

			// get column list of the altered table
			List columns_List = (ArrayList) getAllColumns(sysName, tableName);
			List columns_Names = new ArrayList();

			for (int t = 0; t < actions.size(); t++) {
				columns_Names = new ArrayList();
				AlterTableAction action = (AlterTableAction) actions.get(t);
				// alter action name
				String actionName = action.getAction();
				// alter action parameters
				List parameters = action.getElements();

				if (actionName.equalsIgnoreCase("ADD")) {
					// add a column
					for (int i = 0; i < columns_List.size(); i++) {
						SelectColumn column = (SelectColumn) columns_List.get(i);
						columns_Names.add(column.expression.text().toString());
					}
					ColumnDef cdef = (ColumnDef) parameters.get(0);
					columns_Names.add(cdef.name);
				} else if (actionName.equalsIgnoreCase("DROP")) {
					// drop a column
					for (int i = 0; i < columns_List.size(); i++) {
						SelectColumn column = (SelectColumn) columns_List.get(i);
						if (!column.expression.text().toString().equalsIgnoreCase((String) parameters.get(0)))
							columns_Names.add(column.expression.text().toString());
					}
				} else if (actionName.equalsIgnoreCase("RENAME")) {
					// rename a column name
					for (int i = 0; i < columns_List.size(); i++) {
						SelectColumn column = (SelectColumn) columns_List.get(i);
						if (!column.expression.text().toString().equalsIgnoreCase((String) parameters.get(0)))
							columns_Names.add(column.expression.text().toString());
					}
					// columns_Names.remove(parameters.get(0));
					columns_Names.add((String) parameters.get(1));
				}
				List tempList = new ArrayList();
				for (int i = 0; i < columns_Names.size(); i++) {
					SQL parser = new SQL(getStream(columns_Names.get(i).toString()));
					SelectColumn selColumn = new SelectColumn();
					Expression e = parser.DoExpression();
					selColumn.expression = e;
					tempList.add(selColumn);
				}
				columns_List = tempList;
			}
			// tableInfo_new.put("columnNameList", columns_Names);
			tableInfo_new.put("columnNameList", new ArrayList(new HashSet(columns_Names)));
		} catch (Exception e) {
			throw new MDSException(e);
		}
		return tableInfo_new;
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
	 */
	public HashMap parseCreateTable(StatementTree sTree) throws MDSException {
		HashMap tableInfo = new HashMap();
		List columnNameList = new ArrayList();
		HashMap map = sTree.map;
		// table name
		String table_name = (String) map.get("table_name");
		// if a volatile table
		Boolean ifVolatile = (Boolean) map.get("volatile");
		// column names of the table
		List columns = new ArrayList();
		columns = (ArrayList) map.get("column_list");
		// table name of the table created from
		String sourceTable = (String) map.get("as_table");
		// subquery of the table created from
		StatementTree select = (StatementTree) map.get("as_clause");

		tableInfo.put("table_name", table_name);
		tableInfo.put("volatile", ifVolatile);
		tableInfo.put("as_table", sourceTable);
		tableInfo.put("as_clause", select);
		tableInfo.put("sqlIndex", new Integer(sqlIndex));

		if (columns.size() > 0) {
			for (int i = 0; i < columns.size(); i++) {
				ColumnDef column = (ColumnDef) columns.get(i);
				String name = (String) column.name;
				columnNameList.add(name);
			}
		}

		else {
			// if created from a subquery
			if (select != null) {
				HashMap asmap = select.map;
				TableSelectExpression selExp = (TableSelectExpression) asmap.get("table_expression");

				List ascolumns = selExp.columns;
				for (int i = 0; i < ascolumns.size(); i++) {
					SelectColumn ascolumn = (SelectColumn) ascolumns.get(i);
					String aliasName = ascolumn.alias;
					if (ascolumn.expression != null) {
						String expStr = ascolumn.expression.text().toString();
						if (aliasName != null) {
							columnNameList.add(aliasName);
						} else {
							columnNameList.add(expStr);
						}
					} else {
						List namesStarImplied = getAllColumnsStarImplied(ascolumn, selExp);
						for (int j = 0; j < namesStarImplied.size(); j++) {
							String name = (String) namesStarImplied.get(j);
							String[] name_array = name.split("\\.");
							if (name_array.length == 2)
								name = name_array[1].trim();
							else if (name_array.length == 3)
								name = name_array[2].trim();
							columnNameList.add(name);
						}
					}
				}
			}
			// if created from a table
			else {
				String sName = "";
				String tName = "";
				String[] sourceTable_array = sourceTable.split("\\.");
				if (sourceTable_array.length == 2) {
					sName = sourceTable_array[0].trim();
					tName = sourceTable_array[1].trim();
				} else if (sourceTable_array.length == 1) {
					tName = sourceTable_array[0].trim();
				}
				List columnList = getAllColumns(sName, tName);
				for (int j = 0; j < columnList.size(); j++) {
					SelectColumn selColumn = (SelectColumn) columnList.get(j);
					String cName = selColumn.expression.text().toString();
					columnNameList.add(cName);
				}
			}
		}
		tableInfo.put("columnNameList", columnNameList);

		return tableInfo;
	}

	/**
	 * parse create table statement; if create table has as clause, must store the source info into
	 * volatileTableSourceList or commonTableSourceList
	 * 
	 * @param STree
	 *            the statement to be parsed.
	 * @return
	 * @throws MDSException
	 */
	public void parseTableWithSelect(StatementTree sTree) throws MDSException {

		// if the table does not have a select clause, need not to deal with
		HashMap map = sTree.map;
		// subquery of the table created from
		StatementTree select = (StatementTree) map.get("as_clause");

		List sourceColumns = new ArrayList();
		List resultColumnList = new ArrayList();

		if (select != null) {
			HashMap tableInfo = parseCreateTable(sTree);

			// table name
			String tableNameWithDot = (String) tableInfo.get("table_name");
			// 根据tableNameWithDot获得sysName和tableName
			String[] temp = tableNameWithDot.split("\\.");
			String tableName = temp[0].trim();
			String sysName = "";
			if (temp.length == 2) {
				sysName = temp[0].trim(); // database name of target table
				tableName = temp[1].trim(); // target table name
			}

			// if a volatile table
			Boolean ifVolatile = (Boolean) tableInfo.get("volatile");
			// column names of the table
			List col_list = (ArrayList) tableInfo.get("columnNameList");
			// sql index of the create table statement
			int index = ((Integer) tableInfo.get("sqlIndex")).intValue();

			// 解析create语句中的select子语句
			HashMap asmap = select.map;
			TableSelectExpression selExp = (TableSelectExpression) asmap.get("table_expression");

			// get all related columns
			List columns = selExp.columns;
			List tempColList = new ArrayList();
			// if having * in select clause, replace it with column names implied
			for (int i = 0; i < columns.size(); i++) {
				SelectColumn tempColumn = (SelectColumn) columns.get(i);
				if (tempColumn.glob_name != null) {
					if (tempColumn.glob_name.indexOf("*") >= 0) {

						List columnsStarImplied = getAllColumnsStarImplied(tempColumn, selExp);
						for (int j = 0; j < columnsStarImplied.size(); j++) {
							String nameStr = (String) columnsStarImplied.get(j);
							SelectColumn selColumn = new SelectColumn();
							SQL parser = new SQL(getStream(nameStr.toString()));
							try {
								Expression e = parser.DoExpression();
								selColumn.expression = e;
								tempColList.add(selColumn);
							} catch (Exception e) {
								e.printStackTrace();
								logger.warn(e.getMessage());
							}
						}
					}
				} else
					tempColList.add(tempColumn);
			}
			columns = tempColList;

			for (int i = 0; i < col_list.size(); i++) {
				String targetCName = (String) col_list.get(i);
				String[] targetCName_array = targetCName.split("\\.");
				if (targetCName_array.length == 2) {
					targetCName = targetCName_array[1].trim();
				} else if (targetCName_array.length == 3) {
					targetCName = targetCName_array[2].trim();
				}

				SelectColumn column = (SelectColumn) columns.get(i);
				List columnNames = new ArrayList();

				String alias = column.alias;
				String exp = "";

				// insert对应的select中的相关字段
				if (column.expression != null) {
					exp = column.expression.text().toString();
					columnNames = getRelatedColumnNames(column);
					if (alias != null)
						exp = exp + " AS " + alias;

					// 调用getSourceColumnList函数获得select中相关字段的源字段
					for (int j = 0; j < columnNames.size(); j++) {
						sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
						for (int z = 0; z < sourceColumns.size(); z++) {
							HashMap sourceColumn = (HashMap) sourceColumns.get(z);
							HashMap resultColumn = new HashMap();
							resultColumn.put("targetDBName", sysName);
							resultColumn.put("targetTName", tableName);
							resultColumn.put("targetCName", targetCName);
							resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
							resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
							resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
							resultColumn.put("expression", (String) sourceColumn.get("expression"));
							resultColumnList.add(resultColumn);
						}
					}
				} else {
					columnNames = getAllColumnsStarImplied(column, selExp);
					// 调用getSourceColumnList函数获得select中相关字段的源字段
					for (int j = 0; j < columnNames.size(); j++) {
						sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
						for (int z = 0; z < sourceColumns.size(); z++) {
							HashMap sourceColumn = (HashMap) sourceColumns.get(z);
							HashMap resultColumn = new HashMap();
							resultColumn.put("targetDBName", sysName);
							resultColumn.put("targetTName", tableName);
							resultColumn.put("targetCName", (String) col_list.get(j));
							resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
							resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
							resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
							resultColumn.put("expression", (String) sourceColumn.get("columnName"));
							resultColumnList.add(resultColumn);
						}
					}
					i = i + columnNames.size() - 1;
				}
			}

			// 处理union语句
			while (selExp.composite_function != -1) {
				selExp = selExp.next_composite;
				// get all related columns
				List columnsInComposite = selExp.columns;
				for (int i = 0; i < col_list.size(); i++) {
					String targetCName = (String) col_list.get(i);
					sourceColumns = new ArrayList();
					SelectColumn column = (SelectColumn) columnsInComposite.get(i);
					List columnNames = new ArrayList();

					String alias = column.alias;
					String exp = "";

					if (column.expression != null) {
						exp = column.expression.text().toString();
						columnNames = getRelatedColumnNames(column);
						if (alias != null)
							exp = exp + " AS " + alias;

						for (int j = 0; j < columnNames.size(); j++) {
							sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
							for (int z = 0; z < sourceColumns.size(); z++) {
								HashMap sourceColumn = (HashMap) sourceColumns.get(z);
								HashMap resultColumn = new HashMap();
								resultColumn.put("targetDBName", sysName);
								resultColumn.put("targetTName", tableName);
								resultColumn.put("targetCName", targetCName);
								resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
								resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
								resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
								resultColumn.put("expression", (String) sourceColumn.get("expression"));
								resultColumnList.add(resultColumn);
							}
						}
					} else {
						columnNames = getAllColumnsStarImplied(column, selExp);
						for (int j = 0; j < columnNames.size(); j++) {
							sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
							for (int z = 0; z < sourceColumns.size(); z++) {
								HashMap sourceColumn = (HashMap) sourceColumns.get(z);
								HashMap resultColumn = new HashMap();
								resultColumn.put("targetDBName", sysName);
								resultColumn.put("targetTName", tableName);
								resultColumn.put("targetCName", (String) col_list.get(j));
								resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
								resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
								resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
								resultColumn.put("expression", (String) sourceColumn.get("columnName"));
								resultColumnList.add(resultColumn);
							}
						}
						i = i + columnNames.size() - 1;
					}
				}
			}

			// 存储create表的源到volatileTableSorceList或commonTableSorceList中
			if (ifVolatile.booleanValue()) {
				HashMap volatileTableSource = new HashMap();
				volatileTableSource.put("sqlIndex", new Integer(index));
				volatileTableSource.put("table_name", tableNameWithDot);
				// System.out.println(": "+resultColumnList);
				volatileTableSource.put("sourceColumnList", new HashSet(resultColumnList));
				volatileTableSourceList.add(volatileTableSource);
			} else {
				HashMap commonTableSource = new HashMap();
				commonTableSource.put("sqlIndex", new Integer(index));
				commonTableSource.put("table_name", tableNameWithDot);
				// System.out.println(": "+resultColumnList);
				commonTableSource.put("sourceColumnList", new HashSet(resultColumnList));
				commonTableSourceList.add(commonTableSource);
			}
		}
	}

	public boolean ifBeyondLimit() {
		boolean beyond = false;
		if (temp_relation_num > 150000)
			beyond = true;
		return beyond;
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
	 */
	public List parseInsert(StatementTree sTree) throws MDSException {
		List sourceColumns = new ArrayList();
		List resultColumns = new ArrayList();
		HashMap map = sTree.map;
		String tableNameWithDot = (String) map.get("table_name"); // target table "database.table"

		String[] temp = tableNameWithDot.split("\\.");
		String tableName = temp[0].trim();
		String sysName = "";
		if (temp.length == 2) {
			sysName = temp[0].trim(); // database name of target table
			tableName = temp[1].trim(); // target table name
		} else {
			//2010-12-02 modify by MarkDong
			if(fileName.indexOf(".pl") == -1) {
				if (!"".equalsIgnoreCase(fileName)) {
					sysName = fileName;
				}
			}
		}
		List col_list = new ArrayList();
		col_list = (ArrayList) map.get("col_list"); // colum names in target table

		// if col_list is null, meaning any default value defined in the CREATE
		// TABLE or CREATE VIEW statement is used
		if (col_list.size() == 0) {
			List colList = getAllColumns(sysName, tableName);
			for (int i = 0; i < colList.size(); i++) {
				SelectColumn tempCol = (SelectColumn) colList.get(i);
				String targetCName = tempCol.expression.text().toString();
				String[] targetCName_array = targetCName.split("\\.");
				if (targetCName_array.length == 2)
					targetCName = targetCName_array[1].trim();
				else if (targetCName_array.length == 3)
					targetCName = targetCName_array[2].trim();
				col_list.add(targetCName);
			}
		}
		

		StatementTree select = (StatementTree) map.get("select"); // query in the insert statement
		if (select != null) {
			HashMap selmap = select.map;
			TableSelectExpression selExp = (TableSelectExpression) selmap.get("table_expression");

			
			// get all related columns
			List columns = new ArrayList();
			columns = selExp.columns;
			List tempColList = new ArrayList();
			// if having * in select clause, replace it with column names implied
			for (int i = 0; i < columns.size(); i++) {
				SelectColumn tempColumn = (SelectColumn) columns.get(i);
				if (tempColumn.glob_name != null) {
					if (tempColumn.glob_name.indexOf("*") >= 0) {

						List columnsStarImplied = getAllColumnsStarImplied(tempColumn, selExp);
						for (int j = 0; j < columnsStarImplied.size(); j++) {
							String nameStr = (String) columnsStarImplied.get(j);
							SelectColumn selColumn = new SelectColumn();
							SQL parser = new SQL(getStream(nameStr.toString()));
							try {
								Expression e = parser.DoExpression();
								selColumn.expression = e;
								tempColList.add(selColumn);
							} catch (Exception e) {
								e.printStackTrace();
								logger.warn(e.getMessage());
							}
						}
						columnsStarImplied = null;
					}
				} else
					tempColList.add(tempColumn);
			}
			columns = tempColList;
			tempColList = null;
			try {
				//关系列表，Where条件及Join中的条件，标记为Source
				List conditions = getConditionsFromTableSelectExp(selExp);
				for(int i=0; i<conditions.size(); i++) {
					Map condition = (Map) conditions.get(i);
					condition.put("type", "Source");
				}

				List resultColumnList = new ArrayList();
				List directColumnList = new ArrayList();		//保存直接关系的列表
				for (int i = 0; i < col_list.size(); i++) {
					String targetCName = (String) col_list.get(i);
					SelectColumn column = (SelectColumn) columns.get(i);
					List columnNames = new ArrayList();
					String alias = column.alias;
					String exp = "";

					// insert对应的select中的相关字段
					if (column.expression != null) {
						exp = column.expression.text().toString();
						columnNames = getRelatedColumnNames(column);
						//判断是否是单一常量
						if(columnNames.size() == 1) {
							String sourceCName = (String) columnNames.get(0);
							if(sourceCName.indexOf("_constant_") == 0) {
								String value = sourceCName.substring(10);
								if(!"".equals(value.trim())) {
									Map condition = new HashMap();
									condition.put("database", sysName);
									condition.put("table", tableName);
									condition.put("column", targetCName);
									condition.put("value", value);
									condition.put("type", "Target");
									conditions.add(condition);
								}
							}
						}
						if (alias != null)
							exp = exp + " AS " + alias;

						// 调用getSourceColumnList函数获得select中相关字段的源字段
						for (int j = 0; j < columnNames.size(); j++) {
							List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
							if(directSourceColumns.size() == 1) {
								//只处理一对一关联的
								Map col = (Map) directSourceColumns.get(0);
								String sourceDBName = (String) col.get("databaseName");
								String sourceTName = (String) col.get("tableName");
								String sourceCName = (String) col.get("columnName");
								if(sourceCName.indexOf("_constant_") == -1) {
									Map directRela = new HashMap();
									directRela.put("targetDBName", sysName);
									directRela.put("targetTName", tableName);
									directRela.put("targetCName", targetCName);
									directRela.put("sourceDBName", sourceDBName);
									directRela.put("sourceTName", sourceTName);
									directRela.put("sourceCName", sourceCName);
									
									directColumnList.add(directRela);
								}
							}

							// long parseStartTime = System.currentTimeMillis();
							sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
							
							// long parseEndTime = System.currentTimeMillis();
							// logger.info("column:"+targetCName);
							// logger.info("get sourceColumn time=>"+(parseEndTime - parseStartTime));

							temp_relation_num = temp_relation_num + sourceColumns.size();
							if (ifBeyondLimit()) {
								blankComment = true;
								return null;
							}
							// long memory = Runtime.getRuntime().totalMemory()/1024;
							// logger.info("memory==="+memory);
							// logger.info("temp_relation_num = "+temp_relation_num);
							for (int z = 0; z < sourceColumns.size(); z++) {
								HashMap sourceColumn = (HashMap) sourceColumns.get(z);
								HashMap resultColumn = new HashMap();
								resultColumn.put("targetDBName", sysName);
								resultColumn.put("targetTName", tableName);
								resultColumn.put("targetCName", targetCName);
								String sourceDBName = (String) sourceColumn.get("databaseName");
								String sourceTName = (String) sourceColumn.get("tableName");
								String sourceCName = (String) sourceColumn.get("columnName");
								resultColumn.put("sourceDBName", sourceDBName);
								resultColumn.put("sourceTName", sourceTName);
								resultColumn.put("sourceCName", sourceCName);
								String expression1 = (String) sourceColumn.get("expression");
								expression1 = expression1.replaceAll("\r", " ");
								expression1 = expression1.replaceAll("\n", " ");
								String commentID = expression1;
								if (sqlNum > sql_num_limit)
									commentID = insertComment(expression1);
								if (blankComment)
									commentID = "Relation transformation is too complex, please refer to the script for detailed info...";

								resultColumn.put("expression", getSubComm(commentID));
								List oldConditions = (List) sourceColumn.get("conditions");
								if(oldConditions != null) resultColumn.put("conditions", oldConditions);
								// logger.info(""+sysName+"."+tableName+"."+targetCName+" <==> "+sourceDBName+"."+sourceTName+"."+sourceCName+"  ===  "+commentID);
								resultColumnList.add(resultColumn);
								
							}
							sourceColumns = null;
						}
					} else {
						columnNames = getAllColumnsStarImplied(column, selExp);
						if (col_list.size() == columnNames.size()) {
							// 调用getSourceColumnList函数获得select中相关字段的源字段
							for (int j = 0; j < columnNames.size(); j++) {
								List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
								if(directSourceColumns.size() == 1) {
									//只处理一对一关联的
									Map col = (Map) directSourceColumns.get(0);
									String sourceDBName = (String) col.get("databaseName");
									String sourceTName = (String) col.get("tableName");
									String sourceCName = (String) col.get("columnName");
									if(sourceCName.indexOf("_constant_") == -1) {
										Map directRela = new HashMap();
										directRela.put("targetDBName", sysName);
										directRela.put("targetTName", tableName);
										directRela.put("targetCName", targetCName);
										directRela.put("sourceDBName", sourceDBName);
										directRela.put("sourceTName", sourceTName);
										directRela.put("sourceCName", sourceCName);
										
										directColumnList.add(directRela);
									}
								}

								sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
								temp_relation_num = temp_relation_num + sourceColumns.size();
								if (ifBeyondLimit()) {
									blankComment = true;
									return null;
								}
								for (int z = 0; z < sourceColumns.size(); z++) {
									HashMap sourceColumn = (HashMap) sourceColumns.get(z);
									HashMap resultColumn = new HashMap();
									resultColumn.put("targetDBName", sysName);
									resultColumn.put("targetTName", tableName);
									resultColumn.put("targetCName", (String) col_list.get(j));
									String sourceDBName = (String) sourceColumn.get("databaseName");
									String sourceTName = (String) sourceColumn.get("tableName");
									String sourceCName = (String) sourceColumn.get("columnName");
									resultColumn.put("sourceDBName", sourceDBName);
									resultColumn.put("sourceTName", sourceTName);
									resultColumn.put("sourceCName", sourceCName);
									String expression1 = (String) sourceColumn.get("expression");
									expression1 = expression1.replaceAll("\r", " ");
									expression1 = expression1.replaceAll("\n", " ");
									String commentID = expression1;
									if (sqlNum > sql_num_limit)
										commentID = insertComment(expression1);
									if (blankComment)
										commentID = "Relation transformation is too complex, please refer to the script for detailed info...";
									resultColumn.put("expression", getSubComm(commentID));
									List oldConditions = (List) sourceColumn.get("conditions");
									if(oldConditions != null) resultColumn.put("conditions", oldConditions);
									resultColumnList.add(resultColumn);
								}
								sourceColumns = null;
							}
							i = i + columnNames.size() - 1;
						} else {
							throw new MDSException("==> colum number of view: " + tableName
									+ " is not consistent with that of its selecting table!");
						}
					}
				}

				//过滤conditions
				conditions = cleanConditions(conditions);
				resultColumns.addAll(calcConstCondition(resultColumnList, directColumnList, conditions));
				
			} catch (MDSException e) {
				throw e;
			} catch (Exception e) {
				throw new MDSException("==>colum number of insert is not consistent with that of its selecting table!", e);
			}

			// 处理union语句
			while (selExp.composite_function != -1) {
				List resultColumnList = new ArrayList();

				selExp = selExp.next_composite;
				// get all related columns
				//关系列表
				List conditions = getConditionsFromTableSelectExp(selExp);
				List columnsInComposite = selExp.columns;
				List directColumnList = new ArrayList();		//保存直接关系的列表
				for (int i = 0; i < col_list.size(); i++) {
					String targetCName = (String) col_list.get(i);
					sourceColumns = new ArrayList();
					SelectColumn column = (SelectColumn) columnsInComposite.get(i);
					List columnNames = new ArrayList();

					String alias = column.alias;
					String exp = "";

					if (column.expression != null) {
						exp = column.expression.text().toString();
						columnNames = getRelatedColumnNames(column);
						
						//判断是否是单一常量
						if(columnNames.size() == 1) {
							String sourceCName = (String) columnNames.get(0);
							if(sourceCName.indexOf("_constant_") == 0) {
								String value = sourceCName.substring(10);
								if(!"".equals(value.trim())) {
									Map condition = new HashMap();
									condition.put("database", sysName);
									condition.put("table", tableName);
									condition.put("column", targetCName);
									condition.put("value", value);
									condition.put("type", "Target");
									conditions.add(condition);
								}
							}
						}
						
						if (alias != null)
							exp = exp + " AS " + alias;

						for (int j = 0; j < columnNames.size(); j++) {
							List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
							if(directSourceColumns.size() == 1) {
								//只处理一对一关联的
								Map col = (Map) directSourceColumns.get(0);
								String sourceDBName = (String) col.get("databaseName");
								String sourceTName = (String) col.get("tableName");
								String sourceCName = (String) col.get("columnName");
								if(sourceCName.indexOf("_constant_") == -1) {
									Map directRela = new HashMap();
									directRela.put("targetDBName", sysName);
									directRela.put("targetTName", tableName);
									directRela.put("targetCName", targetCName);
									directRela.put("sourceDBName", sourceDBName);
									directRela.put("sourceTName", sourceTName);
									directRela.put("sourceCName", sourceCName);
									
									directColumnList.add(directRela);
								}

							}

							sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
							temp_relation_num = temp_relation_num + sourceColumns.size();
							if (ifBeyondLimit()) {
								blankComment = true;
								return null;
							}
							for (int z = 0; z < sourceColumns.size(); z++) {
								HashMap sourceColumn = (HashMap) sourceColumns.get(z);
								HashMap resultColumn = new HashMap();
								resultColumn.put("targetDBName", sysName);
								resultColumn.put("targetTName", tableName);
								resultColumn.put("targetCName", targetCName);
								resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
								resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
								resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
								String expression1 = (String) sourceColumn.get("expression");
								expression1 = expression1.replaceAll("\r", " ");
								expression1 = expression1.replaceAll("\n", " ");
								String commentID = expression1;
								if (sqlNum > sql_num_limit)
									commentID = insertComment(expression1);
								if (blankComment)
									commentID = "Relation transformation is too complex, please refer to the script for detailed info...";
								resultColumn.put("expression", getSubComm(commentID));
								List oldConditions = (List) sourceColumn.get("conditions");
								if(oldConditions != null) resultColumn.put("conditions", oldConditions);
								resultColumnList.add(resultColumn);
							}
							sourceColumns = null;
						}
					} else {
						columnNames = getAllColumnsStarImplied(column, selExp);
						if (col_list.size() == columnNames.size()) {
							for (int j = 0; j < columnNames.size(); j++) {
								List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
								if(directSourceColumns.size() == 1) {
									//只处理一对一关联的
									Map col = (Map) directSourceColumns.get(0);
									String sourceDBName = (String) col.get("databaseName");
									String sourceTName = (String) col.get("tableName");
									String sourceCName = (String) col.get("columnName");
									if(sourceCName.indexOf("_constant_") == -1) {
										Map directRela = new HashMap();
										directRela.put("targetDBName", sysName);
										directRela.put("targetTName", tableName);
										directRela.put("targetCName", targetCName);
										directRela.put("sourceDBName", sourceDBName);
										directRela.put("sourceTName", sourceTName);
										directRela.put("sourceCName", sourceCName);
										
										directColumnList.add(directRela);
									}

								}
								sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
								temp_relation_num = temp_relation_num + sourceColumns.size();
								if (ifBeyondLimit()) {
									blankComment = true;
									return null;
								}
								for (int z = 0; z < sourceColumns.size(); z++) {
									HashMap sourceColumn = (HashMap) sourceColumns.get(z);
									HashMap resultColumn = new HashMap();
									resultColumn.put("targetDBName", sysName);
									resultColumn.put("targetTName", tableName);
									resultColumn.put("targetCName", (String) col_list.get(j));
									resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
									resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
									resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
									String expression1 = (String) sourceColumn.get("expression");
									expression1 = expression1.replaceAll("\r", " ");
									expression1 = expression1.replaceAll("\n", " ");
									String commentID = expression1;
									if (sqlNum > sql_num_limit)
										commentID = insertComment(expression1);
									if (blankComment)
										commentID = "Relation transformation is too complex, please refer to the script for detailed info...";
									resultColumn.put("expression", getSubComm(commentID));
									List oldConditions = (List) sourceColumn.get("conditions");
									if(oldConditions != null) resultColumn.put("conditions", oldConditions);
									resultColumnList.add(resultColumn);
								}
								sourceColumns = null;
							}
							i = i + columnNames.size() - 1;
						} else {
							throw new MDSException("==> colum number of view: " + tableName
									+ " is not consistent with that of its selecting table!");
						}
					}
				}
				
				//过滤conditions
				conditions = cleanConditions(conditions);
				resultColumns.addAll(calcConstCondition(resultColumnList, directColumnList, conditions));
			}
		} else {
			List resultColumnList = new ArrayList();
			// insert...values...
			List data_list = new ArrayList();
			data_list = (ArrayList) map.get("data_list");
			List values = (ArrayList) data_list.get(0);
			if (col_list.size() == values.size()) {
				for (int i = 0; i < col_list.size(); i++) {
					String targetCName = (String) col_list.get(i);

					HashMap resultColumn = new HashMap();
					resultColumn.put("targetDBName", sysName);
					resultColumn.put("targetTName", tableName);
					resultColumn.put("targetCName", targetCName);
					resultColumn.put("sourceDBName", "");
					resultColumn.put("sourceTName", "");
					resultColumn.put("sourceCName", "_constant");
					Expression expression = (Expression) values.get(i);
					String expression1 = expression.text().toString();
					expression1 = expression1.replaceAll("\r", " ");
					expression1 = expression1.replaceAll("\n", " ");
					resultColumn.put("expression", expression1);

					resultColumnList.add(resultColumn);
				}
			} else {
				throw new MDSException("==> colum number of table: " + tableName
						+ " is not consistent with that of its values clause!");
			}
			
			resultColumns.addAll(resultColumnList);
		}
		
		HashMap volatileTableSource = new HashMap();
		HashMap commonTableSource = new HashMap();
		int index = -1;
		index = volatileIndex(tableNameWithDot);
		// add all the valatile table resources into volatileTableSourceList
		if (index >= 0) {
			volatileTableSource.put("sqlIndex", new Integer(index));
			volatileTableSource.put("table_name", tableNameWithDot);
			// System.out.println(": "+resultColumnList);
			volatileTableSource.put("sourceColumnList", new HashSet(resultColumns));
			//TODO:这里加入常量字段后在放到volatileTableSourceList中
			volatileTableSource.put("ConstConditions", null);
			
			volatileTableSourceList.add(volatileTableSource);
			volatileTableSource = null;
			resultColumns = new ArrayList();
			return resultColumns;
		}

		else
			index = commonIndex(tableNameWithDot);

		if (index >= 0) {
			commonTableSource.put("sqlIndex", new Integer(index));
			commonTableSource.put("table_name", tableNameWithDot);
			// System.out.println(": "+resultColumnList);
			commonTableSource.put("sourceColumnList", new HashSet(resultColumns));
			//TODO:这里加入常量字段后再放到commonTableSourceList中
			commonTableSourceList.add(commonTableSource);
			commonTableSource = null;
			resultColumns = new ArrayList();
			return resultColumns;
		}
		map = null;
		select = null;
		sourceColumns = null;
		return new ArrayList(new HashSet(resultColumns));
		// return resultColumnList;
	}

	/**
	 * 处理
	 * @param resultColumnList
	 * @param conditions
	 * @return
	 */
	private List calcConstCondition(List resultColumnList, List directColumnRelas, List conditions) {
		//首先将所有常量条件为源字段的扩展出目标字段
		List result = new ArrayList();
		
		try {
			for(int i=0; i<resultColumnList.size(); i++) {
				Map resultColumn = (Map) resultColumnList.get(i);
				List oldConditions = (List) resultColumn.get("conditions");
				List newConditions = mergeConditions(oldConditions, conditions, directColumnRelas);
				resultColumn.put("conditions", newConditions);
				result.add(resultColumn);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 合并旧关系和新关系
	 */
	private List mergeConditions(List oldConditions, List conditions, List directColumnRelas) {
		if(oldConditions == null) oldConditions = new ArrayList();
		if(conditions == null) conditions = new ArrayList();
		if(directColumnRelas == null) directColumnRelas = new ArrayList();
		
		List newConditions = new ArrayList();
		for(int i=0; i<conditions.size(); i++) {
			Map condition = (Map) conditions.get(i);
			String isVolatile = (String) condition.get("volatile");
			String type = (String) condition.get("type");
			String database = (String) condition.get("database");
			String table = (String) condition.get("table");
			String column = (String) condition.get("column");
			String value = (String) condition.get("value");
			
			if(isVolatile.equals("TRUE") && type.equals("Source")) {
				boolean transed = false;
				for(int j=0; j<oldConditions.size(); j++) {
					Map oldCondition = (Map) oldConditions.get(j);
					String oldIsVolatile = (String) oldCondition.get("volatile");
					String oldType = (String) oldCondition.get("type");
					String oldDatabase = (String) oldCondition.get("database");
					String oldTable = (String) oldCondition.get("table");
					String oldColumn = (String) oldCondition.get("column");
					String oldValue = (String) oldCondition.get("value");
					
					if(database.equalsIgnoreCase(oldDatabase) 
							&& table.equalsIgnoreCase(oldTable) 
							&& column.equalsIgnoreCase(oldColumn)
							&& value.equalsIgnoreCase(oldValue)
							&& oldType.equals("Target")) {
						//替换为实际的Target字段
						for(int k=0; k<directColumnRelas.size(); k++) {
							Map columnRela = (Map) directColumnRelas.get(k);
							String sourceDBName = (String) columnRela.get("sourceDBName");
							String sourceTName = (String) columnRela.get("sourceTName");
							String sourceCName = (String) columnRela.get("sourceCName");
							
							if(sourceDBName.equalsIgnoreCase(oldDatabase)
									&& sourceTName.equalsIgnoreCase(oldTable)
									&& sourceCName.equalsIgnoreCase(oldColumn)) {
								String targetDBName = (String) columnRela.get("targetDBName");
								String targetTName = (String) columnRela.get("targetTName");
								String targetCName = (String) columnRela.get("targetCName");
								
								Map newCondition = new HashMap();
								newCondition.put("database", targetDBName);
								newCondition.put("table", targetTName);
								newCondition.put("column", targetCName);
								newCondition.put("value", oldValue);
								newCondition.put("type", "Target");
								if("".equals(targetDBName.trim()))
									newCondition.put("volatile", "TRUE");
								else
									newCondition.put("volatile", "FALSE");
								newConditions.add(newCondition);
								transed = true;
								oldConditions.remove(j);
								j--;
							}
						}
					}
				}	
				if(!transed) newConditions.add(condition);
 
			} else {
				newConditions.add(condition);
			}
			
		} 
		
		//处理剩余的旧关系
		for(int j=0; j<oldConditions.size(); j++) {
			Map condition = (Map) oldConditions.get(j);
			String isVolatile = (String) condition.get("volatile");
			String type = (String) condition.get("type");
			String database = (String) condition.get("database");
			String table = (String) condition.get("table");
			String column = (String) condition.get("column");
			String value = (String) condition.get("value");

			if(isVolatile.equalsIgnoreCase("TRUE")
					&& type.equalsIgnoreCase("Target")) {
				for(int k=0; k<directColumnRelas.size(); k++) {
					Map columnRela = (Map) directColumnRelas.get(k);
					String sourceDBName = (String) columnRela.get("sourceDBName");
					if(sourceDBName == null) sourceDBName = "";
					String sourceTName = (String) columnRela.get("sourceTName");
					String sourceCName = (String) columnRela.get("sourceCName");
					
					if(sourceDBName.equalsIgnoreCase(database)
							&& sourceTName.equalsIgnoreCase(table)
							&& sourceCName.equalsIgnoreCase(column)) {
						String targetDBName = (String) columnRela.get("targetDBName");
						String targetTName = (String) columnRela.get("targetTName");
						String targetCName = (String) columnRela.get("targetCName");
						
						Map newCondition = new HashMap();
						newCondition.put("database", targetDBName);
						newCondition.put("table", targetTName);
						newCondition.put("column", targetCName);
						newCondition.put("value", value);
						newCondition.put("type", "Target");
						if("".equals(targetDBName.trim()))
							newCondition.put("volatile", "TRUE");
						else
							newCondition.put("volatile", "FALSE");
						newConditions.add(newCondition);
					}
				}
				
			} else {
				newConditions.add(condition);
			}
		}
		return newConditions;
	}

	/**
	 * 过滤Conditions列表中的值，将不是Const_Cd_Column中指定字段的值过滤	
	 */
	private List cleanConditions(List conditions) {
		List cleaned = new ArrayList();
		
		for(int i=0; i < conditions.size(); i++) {
			Map condition = (Map) conditions.get(i);
			
			String database = (String) condition.get("database");
			String table = (String) condition.get("table");
			String column = (String) condition.get("column");
			
			String full_table;
			if(database != null && database.trim().length() > 0) {
				full_table = database.toUpperCase() + "." + table.toUpperCase();
			} else {
				full_table = table.toUpperCase();
			}
			
			//从ColMap中获取
			boolean isConst = false;
			List cols = (List) this.colMap.get(full_table);
			if(cols != null) {
				for(int j=0; j<cols.size(); j++) {
					Map col = (Map) cols.get(j);
					String column_name = (String) col.get("COLUMNNAME");
					if(column.equalsIgnoreCase(column_name)) {
						String isConstCd = (String) col.get("IS_CONST_CD");
						if(isConstCd.equalsIgnoreCase("Y")) {
							isConst = true;
							condition.put("volatile", "FALSE");
						}
						break;
					}
				}
			}
			
			if(isConst) {
				cleaned.add(condition);
				continue;
			}
			
			//如果在普通表中没有找到，在volatile中查询
			int sqlIndex = this.volatileIndex(full_table);
			if(sqlIndex >= 0) {
				Map tableInfo = null;
				for(int j=0; j<this.volatileTableList.size(); j++) {
					tableInfo = (Map) this.volatileTableList.get(j);
					Integer index = (Integer) tableInfo.get("sqlIndex");
					if(index.intValue() == sqlIndex) break;
				}
				
				List const_cols = (List) tableInfo.get("const_column_list");
				if(const_cols != null) {
					for(int j=0; j<const_cols.size(); j++) {
						String const_col_name = (String) const_cols.get(j);
						if(const_col_name.equalsIgnoreCase(column)) {
							isConst = true;
							condition.put("volatile", "TRUE");
							break;
						}
					}
				}
				
			}
			if(isConst) {
				cleaned.add(condition);
				continue;
			}
			
			//如果还没找到，继续在common中查询
			sqlIndex = this.commonIndex(full_table);
			if(sqlIndex >= 0) {
				Map tableInfo = null;
				for(int j=0; j<this.commonTableList.size(); j++) {
					tableInfo = (Map) this.commonTableList.get(j);
					Integer index = (Integer) tableInfo.get("sqlIndex");
					if(index.intValue() == sqlIndex) break;
				}

        if(tableInfo == null) break;
				List const_cols = (List) tableInfo.get("const_column_list");
				if(const_cols != null) {
					for(int j=0; j<const_cols.size(); j++) {
						String const_col_name = (String) const_cols.get(j);
						if(const_col_name.equalsIgnoreCase(column)) {
							isConst = true;
							condition.put("volatile", "TRUE");
							break;
						}
					}
				}
			}
			if(isConst) {
				cleaned.add(condition);
				continue;
			}
			
		}
		
		return cleaned;
	}

	private List getConditionsFromTableSelectExp(TableSelectExpression selExp) {
		List conditions = new ArrayList();
		//首先处理Where条件
		SearchExpression where = selExp.where_clause;
		Expression exp = where.getFromExpression();
		if(exp != null) {
			conditions.addAll(getConditionFromExp(exp));
		}

		FromClause fromClause = selExp.from_clause;
		if(fromClause != null) {
			JoiningSet joinSet = fromClause.getJoinSet();
			if(joinSet != null) {
				List join_set = joinSet.join_set;
				
				for(int j=0; j<join_set.size(); j++) {
					Object joinObj = join_set.get(j);
					if(joinObj instanceof JoinPart) {
						JoinPart joinPart = (JoinPart) joinObj;
						Expression joinexp = joinPart.getExpression();
						conditions.addAll(getConditionFromExp(joinexp)); 
					}
				}
			}
		}
		
		//过滤conditions中的字段，将别名替换为表名
		List aliases = getTableAliasInFrom(selExp);
		List tables = getTablesInFrom(selExp);
		List result = new ArrayList();
		
		for(int i=0; i<conditions.size(); i++) {
			Map condition = (Map) conditions.get(i);
			String database = (String) condition.get("database");
			String table = (String) condition.get("table");
			String column = (String) condition.get("column");
			
			for(int j=0; j<tables.size(); j++) {
				String realdb = "";
				String realtable = (String) tables.get(j);
				if(realtable.indexOf(".") > 0) {
					String[] tmp = realtable.split("\\.");
					realdb = tmp[0];
					realtable = tmp[1];
				}
				String alias = (String) aliases.get(j);
				
				if(table == null || table.trim().length() == 0) {
					condition.put("database", realdb);
					condition.put("table", realtable);
					break;
				}
				if(realtable.equalsIgnoreCase(table) || alias.equalsIgnoreCase(table)) {
					condition.put("database", realdb);
					condition.put("table", realtable);
					break;
				}
			}
		}
		
		return conditions;
	}
	
	/**
	 * 从表达式中取得常量值的记录
	 */
	private List getConditionFromExp(Expression exp) {
		List conditions = new ArrayList();

		//FIXME : 这里经常会出现ArrayIndexOutOfBoundsException异常，需要研究
		if(exp == null || exp.size() <= 1) 
			return conditions;
		
		Object last_element = exp.last();

		if(last_element instanceof Operator) {
			Operator oper = (Operator) last_element;
			
			if(oper.isLogical()) {
				//AND or OR
				Expression[] subExps = exp.split();
				for(int j=0; j<subExps.length; j++) {
					Expression subExp = subExps[j];
					conditions.addAll(getConditionFromExp(subExp));
				}
			} else if(oper.is("=")) {
				conditions.addAll(getConditionFromEquals(exp));
			} else if(oper.is("= ANY")) {
				Expression[] subExps = exp.split();
				System.out.println(subExps);
			}
		} 
		
		return conditions;
	}
	
	/**
	 * 从Equals表达式中抽取"变量 = 常量"形式的条件
	 */
	private List getConditionFromEquals(Expression exp) {
		List conditions = new ArrayList();
		
		Expression[] subExps = exp.split();
		
		if(subExps[0].size() > 1) {
			conditions.addAll(getConditionFromExp(subExps[0]));
		} else if(subExps[1].size() > 1) {
			conditions.addAll(getConditionFromExp(subExps[1]));
		} else {
			Object leftOperand = subExps[0].elementAt(0);
			Object rightOperand = subExps[1].elementAt(0);
			
			if(leftOperand instanceof Variable && rightOperand instanceof TObject) {
				Variable var = (Variable) leftOperand;
				TObject obj = (TObject) rightOperand;
				
				TableName tableName = var.getTableName();
				
				List values = getConstValuesFromTObject(obj);
				
				for(int j=0; j<values.size(); j++) {
					String value = (String) values.get(j);
					Map condition = new HashMap();
					if(tableName != null) {
						condition.put("database", tableName.getSchema());
						condition.put("table", tableName.getName());
					}
					condition.put("column", var.getName());
					condition.put("value", value);
					conditions.add(condition);
				}
			}
		}
		
		return conditions;
	}
	
	
	/**
	 * 取得TObject中的常量值列表
	 */
	private List getConstValuesFromTObject(TObject obj) {
		List values = new ArrayList();
		TType type = obj.getTType();
		if(type.equals(TType.ARRAY_TYPE)) {
			Expression[] elements = (Expression[]) obj.getObject();
			for(int j=0; j<elements.length; j++) {
				Expression exp = elements[j];
				if(exp.size() != 1)	continue;
				Object subObject = exp.elementAt(0);
				if(!(subObject instanceof TObject)) continue;
				TObject subTObj = (TObject) subObject;
				if(subTObj.getTType() instanceof TArrayType) continue;
				values.add(subTObj.toString());
			}

		} else {
			values.add(obj.toString());
		}
		
		return values;
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
	 * 
	 *            We can get the entire tree info from its map(Type: HashMap). Because this is a "create view"
	 *            statement, it points to 'com.teradata.sqlparser.interpret.ViewManager'. The map's stucture:
	 *            "view_name" view name. Type: String "type" 'create' "column_list" the view columns. Type: ArrayList
	 *            "select_expression" select clause of the created view statement. Type: TableSelectExpression
	 * 
	 * @return
	 * @throws MDSException
	 */
	public List parseCreateView(StatementTree sTree, String sysName) throws MDSException {
		List sourceColumns = new ArrayList();
		List resultColumns = new ArrayList();
		List directColumnList = new ArrayList();		//保存直接关系的列表
		
		fileName = sysName;
		HashMap map = sTree.map;
		// view name
		String viewNameWithDot = (String) map.get("view_name");
		// view columns
		List col_list = new ArrayList();
		col_list = (ArrayList) map.get("column_list");
		// view source table definition
		TableSelectExpression selExp = (TableSelectExpression) map.get("select_expression");
		// get all related columns
		List columns = new ArrayList();
		columns = selExp.columns;

		String[] temp = viewNameWithDot.split("\\.");
		String targetSysName = ""; // database name of view
		String targetViewName = ""; // view name
		if (temp.length == 1)// 只有view的名称，不带库名前缀
		{
			targetSysName = sysName.toUpperCase();
			targetViewName = temp[0].trim();
		} else {
			targetSysName = temp[0].trim();
			targetViewName = temp[1].trim();
		}

		// if no view columns defined, implies all columns in select expression
		if (col_list.size() == 0) {
			List allColumns = getAllColumns(targetSysName, targetViewName);
			for (int i = 0; i < allColumns.size(); i++) {
				SelectColumn selColumn = (SelectColumn) allColumns.get(i);
				String columnName = selColumn.expression.text().toString();
				col_list.add(columnName);
			}
		}
		try {
			List resultColumnList = new ArrayList();
			//关系列表，Where条件及Join中的条件，标记为Source
			List conditions = getConditionsFromTableSelectExp(selExp);
			for(int i=0; i<conditions.size(); i++) {
				Map condition = (Map) conditions.get(i);
				condition.put("type", "Source");
			}
			
			// if ( col_list.size() == columns.size()){
			for (int i = 0; i < col_list.size(); i++) {
				SelectColumn column = (SelectColumn) columns.get(i);
				List columnNames = new ArrayList();

				String alias = column.alias;
				String exp = "";
				// view对应的select中的相关字段
				if (column.expression != null) {
					exp = column.expression.text().toString();
					columnNames = getRelatedColumnNames(column);
					if (alias != null)
						exp = exp + " AS " + alias;

					// 调用getSourceColumnList函数获得select中相关字段的源字段
					for (int j = 0; j < columnNames.size(); j++) {
						List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
						if(directSourceColumns.size() == 1) {
							//只处理一对一关联的
							Map col = (Map) directSourceColumns.get(0);
							String sourceDBName = (String) col.get("databaseName");
							String sourceTName = (String) col.get("tableName");
							String sourceCName = (String) col.get("columnName");
							if(sourceCName.indexOf("_constant_") == -1) {
								Map directRela = new HashMap();
								directRela.put("targetDBName", sysName);
								directRela.put("targetTName", targetViewName);
								directRela.put("targetCName", col_list.get(i));
								directRela.put("sourceDBName", sourceDBName);
								directRela.put("sourceTName", sourceTName);
								directRela.put("sourceCName", sourceCName);
								
								directColumnList.add(directRela);
							}
						}
						
						
						sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
						for (int z = 0; z < sourceColumns.size(); z++) {
							HashMap sourceColumn = (HashMap) sourceColumns.get(z);
							HashMap resultColumn = new HashMap();
							resultColumn.put("targetDBName", targetSysName);
							resultColumn.put("targetTName", targetViewName);
							resultColumn.put("targetCName", col_list.get(i));
							resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
							resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
							resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
							String expression1 = (String) sourceColumn.get("expression");
							expression1 = expression1.replaceAll("\r", " ");
							expression1 = expression1.replaceAll("\n", " ");
							String commentID = expression1;
							if (sqlNum > sql_num_limit)
								commentID = insertComment(expression1);
							resultColumn.put("expression", getSubComm(commentID));
							
							List oldConditions = (List) sourceColumn.get("conditions");
							if(oldConditions != null) resultColumn.put("conditions", oldConditions);
							resultColumnList.add(resultColumn);
							
						}
					}
				} else {
					// exp = "*";
					columnNames = getAllColumnsStarImplied(column, selExp);
					// if (alias != null)
					// exp = exp + " AS " + alias;
					if (col_list.size() == columnNames.size()) {
						// 调用getSourceColumnList函数获得select中相关字段的源字段
						for (int j = 0; j < columnNames.size(); j++) {
							List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
							if(directSourceColumns.size() == 1) {
								//只处理一对一关联的
								Map col = (Map) directSourceColumns.get(0);
								String sourceDBName = (String) col.get("databaseName");
								String sourceTName = (String) col.get("tableName");
								String sourceCName = (String) col.get("columnName");
								if(sourceCName.indexOf("_constant_") == -1) {
									Map directRela = new HashMap();
									directRela.put("targetDBName", sysName);
									directRela.put("targetTName", targetViewName);
									directRela.put("targetCName", col_list.get(i));
									directRela.put("sourceDBName", sourceDBName);
									directRela.put("sourceTName", sourceTName);
									directRela.put("sourceCName", sourceCName);
									
									directColumnList.add(directRela);
								}
							}
							
							
							sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);

							for (int z = 0; z < sourceColumns.size(); z++) {
								HashMap sourceColumn = (HashMap) sourceColumns.get(z);
								HashMap resultColumn = new HashMap();
								resultColumn.put("targetDBName", targetSysName);
								resultColumn.put("targetTName", targetViewName);
								resultColumn.put("targetCName", col_list.get(j));
								resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
								resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
								resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
								resultColumn.put("expression", (String) sourceColumn.get("columnName"));
								List oldConditions = (List) sourceColumn.get("conditions");
								if(oldConditions != null) resultColumn.put("conditions", oldConditions);
								resultColumnList.add(resultColumn);
							}
						}
						i = i + columnNames.size() - 1;
					} else {
						throw new MDSException("==> colum number of view: " + viewNameWithDot
								+ " is not consistent with that of its selecting table!");
					}
				}
			}
			//过滤conditions
			conditions = cleanConditions(conditions);
			resultColumns.addAll(calcConstCondition(resultColumnList, directColumnList, conditions));
			
		} catch (Exception e) {
			throw new MDSException(
					"View Parse Error", e);
		}
		// }
		// else {
		// throw new
		// MDSException("==>column number of view is not consistent with that of its selecting table OR column is not in selecting table!");
		// }
		// ArrayList aliasList = selExp.aliasList;
		// 处理union语句
		while (selExp.composite_function != -1) {
			List resultColumnList = new ArrayList();
			selExp = selExp.next_composite;

			//关系列表，Where条件及Join中的条件，标记为Source
			List conditions = getConditionsFromTableSelectExp(selExp);
			for(int i=0; i<conditions.size(); i++) {
				Map condition = (Map) conditions.get(i);
				condition.put("type", "Source");
			}

			// selExp.aliasList=aliasList;
			// get all related columns
			List columnsInComposite = selExp.columns;

			for (int i = 0; i < col_list.size(); i++) {
				sourceColumns = new ArrayList();
				SelectColumn column = (SelectColumn) columnsInComposite.get(i);
				List columnNames = new ArrayList();

				String alias = column.alias;
				String exp = "";
				// view对应的select中的相关字段
				if (column.expression != null) {
					exp = column.expression.text().toString();
					columnNames = getRelatedColumnNames(column);
					if (alias != null)
						exp = exp + " AS " + alias;

					// 调用getSourceColumnList函数获得select中相关字段的源字段
					for (int j = 0; j < columnNames.size(); j++) {
						List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
						if(directSourceColumns.size() == 1) {
							//只处理一对一关联的
							Map col = (Map) directSourceColumns.get(0);
							String sourceDBName = (String) col.get("databaseName");
							String sourceTName = (String) col.get("tableName");
							String sourceCName = (String) col.get("columnName");
							if(sourceCName.indexOf("_constant_") == -1) {
								Map directRela = new HashMap();
								directRela.put("targetDBName", sysName);
								directRela.put("targetTName", targetViewName);
								directRela.put("targetCName", col_list.get(i));
								directRela.put("sourceDBName", sourceDBName);
								directRela.put("sourceTName", sourceTName);
								directRela.put("sourceCName", sourceCName);
								
								directColumnList.add(directRela);
							}
						}
						sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
						for (int z = 0; z < sourceColumns.size(); z++) {
							HashMap sourceColumn = (HashMap) sourceColumns.get(z);
							HashMap resultColumn = new HashMap();
							resultColumn.put("targetDBName", targetSysName);
							resultColumn.put("targetTName", targetViewName);
							resultColumn.put("targetCName", col_list.get(i));
							resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
							resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
							resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
							String expression1 = (String) sourceColumn.get("expression");
							expression1 = expression1.replaceAll("\r", " ");
							expression1 = expression1.replaceAll("\n", " ");
							String commentID = expression1;
							if (sqlNum > sql_num_limit)
								commentID = insertComment(expression1);
							resultColumn.put("expression", getSubComm(commentID));
							List oldConditions = (List) sourceColumn.get("conditions");
							if(oldConditions != null) resultColumn.put("conditions", oldConditions);
							resultColumnList.add(resultColumn);
						}
					}
				} else {
					columnNames = getAllColumnsStarImplied(column, selExp);
					if (col_list.size() == columnNames.size()) {
						// 调用getSourceColumnList函数获得select中相关字段的源字段
						for (int j = 0; j < columnNames.size(); j++) {
							List directSourceColumns = getDirectSourceColumnList((String) columnNames.get(j), exp, selExp);
							if(directSourceColumns.size() == 1) {
								//只处理一对一关联的
								Map col = (Map) directSourceColumns.get(0);
								String sourceDBName = (String) col.get("databaseName");
								String sourceTName = (String) col.get("tableName");
								String sourceCName = (String) col.get("columnName");
								if(sourceCName.indexOf("_constant_") == -1) {
									Map directRela = new HashMap();
									directRela.put("targetDBName", sysName);
									directRela.put("targetTName", targetViewName);
									directRela.put("targetCName", col_list.get(i));
									directRela.put("sourceDBName", sourceDBName);
									directRela.put("sourceTName", sourceTName);
									directRela.put("sourceCName", sourceCName);
									
									directColumnList.add(directRela);
								}
							}
							sourceColumns = getSourceColumnList((String) columnNames.get(j), exp, selExp);
							for (int z = 0; z < sourceColumns.size(); z++) {
								HashMap sourceColumn = (HashMap) sourceColumns.get(z);
								HashMap resultColumn = new HashMap();
								resultColumn.put("targetDBName", targetSysName);
								resultColumn.put("targetTName", targetViewName);
								resultColumn.put("targetCName", col_list.get(j));
								resultColumn.put("sourceDBName", (String) sourceColumn.get("databaseName"));
								resultColumn.put("sourceTName", (String) sourceColumn.get("tableName"));
								resultColumn.put("sourceCName", (String) sourceColumn.get("columnName"));
								resultColumn.put("expression", (String) sourceColumn.get("columnName"));
								List oldConditions = (List) sourceColumn.get("conditions");
								if(oldConditions != null) resultColumn.put("conditions", oldConditions);
								resultColumnList.add(resultColumn);
							}
						}
						i = i + columnNames.size() - 1;
					} else {
						throw new MDSException("==> colum number of view: " + viewNameWithDot
								+ " is not consistent with that of its selecting table!");
					}
				}

			}
			//过滤conditions
			conditions = cleanConditions(conditions);
			resultColumns.addAll(calcConstCondition(resultColumnList, directColumnList, conditions));
		}

		return new ArrayList(new HashSet(resultColumns));
	}

	/**
	 * get all table name list in a select statement;
	 * 
	 * @param selExp
	 *            select Expression.
	 * @return
	 */
	public List getTablesInFrom(TableSelectExpression selExp) {
		FromClause fromclause = selExp.from_clause;
		List def_list = new ArrayList();
		def_list = fromclause.def_list;
		List tablesInFrom = new ArrayList();
		for (int i = 0; i < def_list.size(); i++) {
			FromTableDef table_def = (FromTableDef) def_list.get(i);
			tablesInFrom.add(table_def.getName());
		}
		return tablesInFrom;
	}

	/**
	 * get all table alias list in a select statement;
	 * 
	 * @param selExp
	 *            select Expression.
	 * @return
	 */
	public List getTableAliasInFrom(TableSelectExpression selExp) {
		FromClause fromclause = new FromClause();
		fromclause = selExp.from_clause;
		List def_list = new ArrayList();
		def_list = fromclause.def_list;
		List tableAliasInFrom = new ArrayList();
		for (int i = 0; i < def_list.size(); i++) {
			FromTableDef table_def = (FromTableDef) def_list.get(i);
			if (table_def.getAlias() != null) {
				tableAliasInFrom.add(table_def.getAlias());
			} else
				tableAliasInFrom.add(new String(""));
		}
		return tableAliasInFrom;
	}

	/**
	 * get all columns in a table or a view;
	 * 
	 * @param sysName
	 *            database name; tableName table name or view name
	 * @return
	 * @throws MDSException
	 */
	public List getAllColumns(String sysName, String tableName) throws MDSException {
		List columnList = new ArrayList();
		// String columnName;
		// String order;
		// if a volatile table
		try {
			if ("".equals(sysName)) {
				int volatileIndex = volatileIndex(tableName);
				for (int j = 0; j < volatileTableList.size(); j++) {
					HashMap volatileTable = (HashMap) volatileTableList.get(j);
					Integer index = (Integer) volatileTable.get("sqlIndex");
					if (index.intValue() == volatileIndex) {
						List colList = new ArrayList();
						colList = (ArrayList) volatileTable.get("columnNameList");

						for (int i = 0; i < colList.size(); i++) {
							String column = (String) colList.get(i);
							SelectColumn selColumn = new SelectColumn();
							SQL parser = new SQL(getStream(column.toString()));
							Expression e = parser.DoExpression();
							selColumn.expression = e;
							columnList.add(selColumn);
						}
						return columnList;
					}
				}
			}

			// if table created in script
			int commonIndex = commonIndex(sysName + "." + tableName);
			for (int j = 0; j < commonTableList.size(); j++) {
				HashMap commonTable = (HashMap) commonTableList.get(j);
				Integer index = (Integer) commonTable.get("sqlIndex");
				if (index.intValue() == commonIndex) {
					List colList = new ArrayList();
					colList = (ArrayList) commonTable.get("columnNameList");
					for (int i = 0; i < colList.size(); i++) {
						String column = (String) colList.get(i);
						SelectColumn selColumn = new SelectColumn();
						SQL parser = new SQL(getStream(column.toString()));
						Expression e = parser.DoExpression();
						selColumn.expression = e;
						columnList.add(selColumn);
					}
					return columnList;
				}
			}
			if (!"".equalsIgnoreCase(fileName) && fileName.indexOf(".pl") == -1) {
				if ("".equalsIgnoreCase(sysName))
					sysName = fileName.trim();
			}
			/*
			 * ArrayList colNames = new ArrayList(); //放没排序的column name Iterator keys = this.colMap.keySet().iterator();
			 * //logger.info("-->"+sysName+"."+tableName); while(keys.hasNext()) { String key=(String) keys.next();
			 * String[] keyArray = key.split("\\.");
			 * 
			 * 
			 * //logger.info("-->"+key); //判断前缀是否是sysName.tableName //if
			 * (key.indexOf(sysName.toUpperCase()+"."+tableName.toUpperCase())>=0) { if
			 * (keyArray[0].equalsIgnoreCase(sysName)&&keyArray[1].equalsIgnoreCase(tableName)) {
			 * //System.out.println("-->"+key); String[] tmp = key.split("\\."); columnName = tmp[2]; Map col = (Map)
			 * this.colMap.get(key); order = (String) col.get("ORDER_ID"); columnName = order+"."+columnName;
			 * colNames.add(columnName); } } Collections.sort(colNames);
			 * 
			 * for (int i=0; i< colNames.size(); i++) { columnName = ((String) colNames.get(i)).split("\\.")[1];
			 * SelectColumn selColumn = new SelectColumn(); SQL parser = new SQL(getStream(columnName.toString()));
			 * Expression e = parser.DoExpression(); selColumn.expression = e; columnList.add(selColumn); }
			 */

			String key = sysName.toUpperCase() + "." + tableName.toUpperCase();
			List colList = (List) this.colMap.get(key);
			if (colList != null) {
				for (int i = 0; i < colList.size(); i++) {
					Map colInfo = (Map) colList.get(i);
					String columnName = (String) colInfo.get("COLUMNNAME");
					SelectColumn selColumn = new SelectColumn();
					SQL parser = new SQL(getStream(columnName.toString()));
					Expression e = parser.DoExpression();
					selColumn.expression = e;
					columnList.add(selColumn);
				}

				if (columnList.size() == 0) {
					if (sysName == null || sysName.trim().length() == 0)
						throw new MDSException("table : " + tableName + " without database reference!");
					else
						throw new MDSException("table : " + sysName + "." + tableName
								+ " is not loaded or some columns in this table are not loaded!");
				}
			} else
				throw new MDSException("table : " + sysName + "." + tableName
						+ " is not loaded or some columns in this table are not loaded!");
		} catch (ParseException e) {
      e.printStackTrace();
			throw new MDSException(e);
		}
		// System.out.println(columnList);
		return columnList;
	}

	/**
	 * get column name list which star implied;
	 * 
	 * @param column
	 *            SelectColumn; selExp select Expression such as: select * from table1; select a.* from table1 a; select
	 *            db1.a.* from table1;
	 * @return
	 * @throws MDSException
	 */
	public List getAllColumnsStarImplied(SelectColumn column, TableSelectExpression selExp) throws MDSException {
		List columnNames = new ArrayList();
		String glob_name = column.glob_name;
		List tableList = getTablesInFrom(selExp);
		List aliasList = getTableAliasInFrom(selExp);
		String prefixName = ""; // databaseName & tableName
		String dbName = "";
		String tableName = "";
		String[] temp = glob_name.split("\\.");
		// if the column name format is : *
		if (temp.length == 1) {
			prefixName = (String) tableList.get(0);
			String[] prefixName_array = prefixName.split("\\.");
			// if a subquery
			if (prefixName.equalsIgnoreCase((String) aliasList.get(0))) {
				// get columns of the subquery, and return.
				FromClause fromclause = selExp.from_clause;
				List def_list = fromclause.def_list;
				FromTableDef table_def = (FromTableDef) def_list.get(0);
				if (table_def.isSubQueryTable()) {
					TableSelectExpression sub_query = table_def.getTableSelectExpression();
					List columns = sub_query.columns;
					for (int i = 0; i < columns.size(); i++) {
						SelectColumn selColumn = (SelectColumn) columns.get(i);
						String alias = selColumn.alias;
						String cName = "";
						if (alias == null || alias.length() == 0)
							cName = selColumn.expression.text().toString();
						else
							cName = alias;
						columnNames.add(cName);
					}
				}
				return columnNames;
			} else if (prefixName_array.length == 2) {
				// if not a volatile table
				dbName = prefixName_array[0];
				tableName = prefixName_array[1];
			} else if (prefixName_array.length == 1) {
				if (!"".equalsIgnoreCase(fileName) && fileName.indexOf(".pl") == -1) {
					dbName = fileName;
					tableName = prefixName;
					prefixName = dbName + "." + tableName;
				} else {
					// if a volatile table
					dbName = "";
					tableName = prefixName;
				}
			}
		}
		// if the column name format is : tablename.* or alias.*
		if (temp.length == 2) {
			prefixName = temp[0].trim();
			// String[] prefixName_array=prefixName.split("\\.");
			for (int i = 0; i < tableList.size(); i++) {
				// the column name format is: tableAlias.*
				if (prefixName.equalsIgnoreCase((String) tableList.get(i))
						|| prefixName.equalsIgnoreCase((String) aliasList.get(i))) {
					prefixName = (String) tableList.get(i); // 表名
					String aliasName = (String) aliasList.get(i);
					String[] prefixName_array = prefixName.split("\\.");
					if (prefixName_array.length == 2) {
						// select a.* from db1.table1 as a
						dbName = prefixName_array[0];
						tableName = prefixName_array[1];
					} else if (prefixName.equalsIgnoreCase(aliasName)) {
						// if a subquery, select c.* from (sel...)c
						TableSelectExpression subQueryExp = getTempTableDefinition(prefixName, selExp);
						if (subQueryExp != null) {
							List columnList = getColumnNamesInSelect(subQueryExp);
							return columnList;
						}
						// logger.info("==>please replace * with column names! ");
					} else {
						// if a volatile table or a view/macro without dbname, select vt.* from vt;
						if (!"".equalsIgnoreCase(fileName) && fileName.indexOf(".pl") == -1) {
							dbName = fileName;
							tableName = prefixName;
							prefixName = dbName + "." + tableName;
						} else {
							dbName = "";
							// tableName = prefixName_array[0];
							tableName = prefixName;
						}
					}
					break;
				}
			}
		}
		// if the column name format is : databaseName.tableName.*
		if (temp.length == 3) {
			prefixName = temp[0].trim() + "." + temp[1].trim();
			for (int i = 0; i < tableList.size(); i++) {
				if (prefixName.equalsIgnoreCase((String) tableList.get(i))
						|| prefixName.equalsIgnoreCase((String) aliasList.get(i))) {
					prefixName = (String) tableList.get(i);
					String[] prefixName_array = prefixName.split("\\.");
					dbName = prefixName_array[0];
					tableName = prefixName_array[1];
					break;
				}
			}
		}

		// get all columns that a table includes
		List columnList = getAllColumns(dbName, tableName);

		for (int i = 0; i < columnList.size(); i++) {
			SelectColumn selColumn = (SelectColumn) columnList.get(i);
			String cName = prefixName + "." + selColumn.expression.text().toString();
			columnNames.add(cName);
		}

		return columnNames;
	}

	public List getColumnNamesInSelect(TableSelectExpression selExp) throws MDSException {
		List columnNames = new ArrayList();
		List columns = selExp.columns;
		for (int i = 0; i < columns.size(); i++) {
			SelectColumn tempColumn = (SelectColumn) columns.get(i);
			Expression exp = tempColumn.expression;
			if (exp != null) {
				if (exp.allVariables().size() == 0) {
					columnNames.add(exp.text().toString());
				} else {
					Variable var = (Variable) tempColumn.expression.allVariables().get(0);
					if (var.getTableName() != null) {
						String columnName = var.getTableName().getName() + "." + var.getName();
						columnNames.add(columnName);
					} else
						columnNames.add(var.getName());
				}
			} else {
				List tempList = getAllColumnsStarImplied(tempColumn, selExp);
				columnNames.addAll(tempList);
				// for (int t =0; t < tempList.size(); t++)
				// columnNames.add((String)tempList.get(t));

			}
		}
		return columnNames;
	}

	/**
	 * get column list in select statement;
	 * 
	 * @param selExp
	 *            select expression.
	 * @return
	 * @throws MDSException
	 */
	public List getColumnListInSelect(TableSelectExpression selExp) throws MDSException {

		List columns = selExp.columns;
		List columnList = new ArrayList();
		List tempColList = new ArrayList();

		List aliasInFromClause = selExp.aliasList;

		// if having * in select clause, replace it with column names implied
		for (int i = 0; i < columns.size(); i++) {
			SelectColumn tempColumn = (SelectColumn) columns.get(i);
			if (tempColumn.glob_name != null) {
				if (tempColumn.glob_name.indexOf("*") >= 0) {
					List columnsStarImplied = getAllColumnsStarImplied(tempColumn, selExp);
					for (int j = 0; j < columnsStarImplied.size(); j++) {
						String nameStr = (String) columnsStarImplied.get(j);
						SelectColumn selColumn = new SelectColumn();
						SQL parser = new SQL(getStream(nameStr.toString()));

						try {
							Expression e = parser.DoExpression();
							selColumn.expression = e;
							tempColList.add(selColumn);
						} catch (Exception e) {
							e.printStackTrace();
							logger.error(e.getMessage());
						}
					}
				}
			} else
				tempColList.add(tempColumn);
		}
		columns = tempColList;

		for (int i = 0; i < columns.size(); i++) {
			String alias = "";
			SelectColumn column = (SelectColumn) columns.get(i);
			Expression exp = column.expression;
			if (aliasInFromClause.size() != 0)
				alias = (String) aliasInFromClause.get(i);
			else if (column.alias != null)
				alias = column.alias;

			if (exp != null) {
				if (exp.allVariables().size() == 0) {
					HashMap map = new HashMap();
					map.put("columnName", "_constant");
					map.put("columnAlias", alias);
					map.put("columnExpression", exp.text().toString());
					columnList.add(map);
				} else {
					for (int j = 0; j < exp.allVariables().size(); j++) {
						Variable var = (Variable) exp.allVariables().get(j);
						HashMap map = new HashMap();
						if (var.getTableName() != null) {
							String columnName = var.getTableName().getName() + "." + var.getName();
							if (var.getTableName().getSchema() != null) {
								columnName = var.getTableName().getSchema() + "." + var.getTableName().table_name + "."
										+ var.getName();
							}
							map.put("columnName", columnName);
						} else
							map.put("columnName", var.getName());
						map.put("columnAlias", alias);
						map.put("columnExpression", exp.text().toString());
						columnList.add(map);
					}
				}
			}
		}
		return columnList;
	}

	/**
	 * get all columns in an expression;
	 * 
	 * @param exp
	 *            an expression.
	 * @return
	 */
	public List getRelatedColumnNames(Expression exp) {
		List columnNames = new ArrayList();
		if (exp != null) {
			if (exp.allVariables().size() == 0) {
				//TODO：需要增加对Constant的取值
				String columnName = getConstFromExp(exp);
				columnName = "_constant_" + columnName;
				columnNames.add(columnName);
			} else {
				for (int j = 0; j < exp.allVariables().size(); j++) {
					Variable var = (Variable) exp.allVariables().get(j);
					if (var.getTableName() != null) {
						String columnName = var.table_name.toString() + "." + var.getName();
						if (var.getTableName().getSchema() != null) {
							columnName = var.getTableName().getSchema() + "." + var.getTableName().table_name + "."
									+ var.getName();
						}
						columnNames.add(columnName);
					} else
						columnNames.add(var.getName());
				}
			}
		}
		return new ArrayList(new HashSet(columnNames));
	}

	/**
	 * get all columns in a SelectColumn object;
	 * 
	 * @param column
	 *            a SelectColumn object.
	 * @return
	 */
	public List getRelatedColumnNames(SelectColumn column) {
		List columnNames = new ArrayList();
		if (column.expression != null) {
			Expression exp = column.expression;
			if (exp.allVariables().size() == 0) {
				//TODO: 需要增加对Constant的取值
				String columnName = getConstFromExp(exp);
				columnName = "_constant_" + columnName;
				columnNames.add(columnName);
			} else {
				for (int j = 0; j < exp.allVariables().size(); j++) {
					Variable var = (Variable) exp.allVariables().get(j);
					if (var.getTableName() != null) {
						String columnName = var.table_name.toString() + "." + var.getName();
						columnNames.add(columnName);
					} else
						columnNames.add(var.getName());
				}
			}
		}
		return new ArrayList(new HashSet(columnNames));
	}

	/**
	 * 从表达式中提取常量<br>
	 * 对于多个常量的拼接不处理，只处理单一的常量
	 */
	private String getConstFromExp(Expression exp) {
		String const_value = "";
		int element_count = exp.size();
		
		if(element_count == 1) {
			Object element = exp.elementAt(0);
			if(element instanceof FunctionDef) 
				const_value = getConstValueFromFunctionDef((FunctionDef) element);
			
			if(element instanceof TObject)
				const_value = ((TObject) element).toString();
		} else if(element_count > 1) {
			logger.warn("当前不处理多个常量的情况")	;
		}
		return const_value;
	}

	
	private String getConstValueFromFunctionDef(FunctionDef func) {
		String value = "";
		Expression[] exps = func.getParameters();
		if(func.getName().equalsIgnoreCase("TRIM")) {
			Expression subExp = exps[2];
			int element_count = subExp.size();
			
			if(element_count > 1) 
				value = "";
			
			Object element = subExp.elementAt(0);
			if(element instanceof FunctionDef) 
				value = getConstValueFromFunctionDef((FunctionDef)element);
			else if(element instanceof TObject) {
				TObject obj = (TObject) element;
				value = obj.toString();
			}
				
				
		} else if(func.getName().equalsIgnoreCase("COALESCE")) {
			Expression subExp = exps[0];
			Object element = subExp.elementAt(0);      //FIXME 这里会出现IndexOutOfBoundsException异常
			if(element instanceof FunctionDef) 
				value = getConstValueFromFunctionDef((FunctionDef)element);
			else if(element instanceof TObject) {
				TObject obj = (TObject) element;
				value = obj.toString();
			}
		} else if(func.getName().equalsIgnoreCase("CAST")) {
			
			Expression subExp = exps[0];
			if(subExp.size() > 0) { 
				Object element = subExp.elementAt(0);
				if(element instanceof FunctionDef) 
					value = getConstValueFromFunctionDef((FunctionDef)element);
				else if(element instanceof TObject) {
					TObject obj = (TObject) element;
					value = obj.toString();
				}
			}
		}
		return value;
	}
	
	/**
	 * get subquery or a volatile table definition according to given table name;
	 * 
	 * @param tempTableName
	 *            volatile or subquery table name; select select statement.
	 * @return
	 */
	public TableSelectExpression getTempTableDefinition(String tempTableName, TableSelectExpression select) {
		TableSelectExpression tempTableDef = null;

		FromClause fromclause = select.from_clause;
		List def_list = fromclause.def_list;

		int index = -1;
		index = volatileIndex(tempTableName);
		// if the table is a volatile table
		if (index < 0) {
			for (int i = 0; i < def_list.size(); i++) {
				FromTableDef table_def = (FromTableDef) def_list.get(i);
				if (table_def.getAlias() != null && table_def.isSubQueryTable()) {
					if (table_def.getAlias().equalsIgnoreCase(table_def.getName())
							&& table_def.getAlias().equalsIgnoreCase(tempTableName)) {
						tempTableDef = table_def.getTableSelectExpression();
						return tempTableDef;
					}
				}
			}
		}

		return tempTableDef;
	}

	/**
	 * get source table according to the table alias and its select statemednt;
	 * 
	 * @param aliasName
	 *            ; select select statement.
	 * @return
	 */
	public String getSourceTable(String aliasName, TableSelectExpression select) {
		String sourceTableName = null;
		FromClause fromclause = select.from_clause;
		List def_list = fromclause.def_list;
		for (int i = 0; i < def_list.size(); i++) {
			FromTableDef table_def = (FromTableDef) def_list.get(i);
			String alias = table_def.getAlias();
			if (alias != null)
				if (alias.equalsIgnoreCase(aliasName)) {
					sourceTableName = table_def.getName();
					return sourceTableName;
				}
		}
		return sourceTableName;
	}

	/**
	 * get source column list according to given column name, column expression, table name, if Volatile; This function
	 * is used to find source columns of columns whose table created in script file. If "isVolatile" is true, the column
	 * is created in a volatile table.
	 * 
	 * @param qualifyName
	 *            ;columnExpStr,inputTableName,isVolatile.
	 * @return
	 * @throws MDSException
	 */
	public List getSourceColumnList(String qualifyName, String columnExpStr, String inputTableName, boolean isVolatile)
			throws MDSException {
		List sourceColumnList = new ArrayList();
		HashSet columnSet = new HashSet();
		int index = -1;
		if (isVolatile) {
			//TODO:这里是通过临时表找源字段的地方
			index = volatileIndex(inputTableName);
			for (int i = 0; i < volatileTableSourceList.size(); i++) {
				HashMap volatileTable = (HashMap) volatileTableSourceList.get(i);
				Integer volatileIndex = (Integer) volatileTable.get("sqlIndex");
				if (index == volatileIndex.intValue()) {
					columnSet = (HashSet) volatileTable.get("sourceColumnList");
					List columnList = new ArrayList(columnSet);

					for (int j = 0; j < columnList.size(); j++) {
						HashMap column = (HashMap) columnList.get(j);
						HashMap sourceColumn = new HashMap();
						String t_column_name = (String) column.get("targetCName");
						if (!"".equals(column.get("targetTName"))) {
							t_column_name = column.get("targetTName") + "." + t_column_name;
							if (!"".equals(column.get("targetDBName")))
								t_column_name = column.get("targetDBName") + "." + t_column_name;
						}
						if (t_column_name.equalsIgnoreCase(qualifyName)) {
							sourceColumn.put("databaseName", (String) column.get("sourceDBName"));
							String commentID = (String) column.get("expression");
							String exp = commentID + " ==> " + columnExpStr;
							if (sqlNum > sql_num_limit) {
								String relationComm = getComment(commentID);
								exp = relationComm + " ==> " + columnExpStr;
							}
							sourceColumn.put("tableName", (String) column.get("sourceTName"));
							sourceColumn.put("columnName", (String) column.get("sourceCName"));
							sourceColumn.put("expression", exp);
							sourceColumn.put("conditions", (List) column.get("conditions"));
							sourceColumnList.add(sourceColumn);
							// break;
						}
					}
				}
			}
		} else {
			//TODO:这里是通过普通表找源字段的地方
			index = commonIndex(inputTableName);
			for (int i = 0; i < commonTableSourceList.size(); i++) {
				HashMap commonTable = (HashMap) commonTableSourceList.get(i);
				Integer commonIndex = (Integer) commonTable.get("sqlIndex");
				if (index == commonIndex.intValue()) {
					columnSet = (HashSet) commonTable.get("sourceColumnList");
					List columnList = new ArrayList(columnSet);

					for (int j = 0; j < columnList.size(); j++) {
						HashMap column = (HashMap) columnList.get(j);
						HashMap sourceColumn = new HashMap();
						String t_column_name = (String) column.get("targetCName");
						if (!"".equals(column.get("targetTName"))) {
							t_column_name = column.get("targetTName") + "." + t_column_name;
							if (!"".equals(column.get("targetDBName")))
								t_column_name = column.get("targetDBName") + "." + t_column_name;
						}
						if (t_column_name.equalsIgnoreCase(qualifyName)) {
							sourceColumn.put("databaseName", (String) column.get("sourceDBName"));
							String commentID = (String) column.get("expression");
							String exp = commentID + " ==> " + columnExpStr;
							if (sqlNum > sql_num_limit) {
								String relationComm = getComment(commentID);
								exp = relationComm + " ==> " + columnExpStr;
							}
							sourceColumn.put("tableName", (String) column.get("sourceTName"));
							sourceColumn.put("columnName", (String) column.get("sourceCName"));
							sourceColumn.put("expression", exp);
							sourceColumnList.add(sourceColumn);
						}
					}

				}
			}
		}
		return new ArrayList(new HashSet(sourceColumnList));
	}

	private List getDirectSourceColumnList(String qualifyName, String columnExpStr, String inputTableName,
			boolean isVolatile) throws MDSException {
		//FIXME:还没实现
		String[] tmp = qualifyName.split("\\.");
		String col_name = "";
		if(tmp.length == 2) {
			col_name = tmp[1];
		} else {
			col_name = tmp[0];
		}
		List sourceColumnList = new ArrayList();
		int index = -1;
		if (isVolatile) {
			// TODO:这里是通过临时表找源字段的地方
			index = volatileIndex(inputTableName);
			
			if(index != -1) {
				Map volatileTable = null;

				//Map tableInfo = null;
				for(int j=0; j<this.volatileTableList.size(); j++) {
					volatileTable = (Map) this.volatileTableList.get(j);
					Integer sqlIndex = (Integer) volatileTable.get("sqlIndex");
					if(sqlIndex.intValue() == index) break;
				}
				
				List columnList = (List) volatileTable.get("columnNameList");
	
				for (int j = 0; j < columnList.size(); j++) {
					String columnName = (String) columnList.get(j);
					if(columnName.equalsIgnoreCase(col_name)) {
						Map sourceColumn = new HashMap();
						sourceColumn.put("tableName", inputTableName);
						sourceColumn.put("columnName", columnName);
						sourceColumnList.add(sourceColumn);
					}
				}
			}
		} else {
			// TODO:这里是通过普通表找源字段的地方
			index = commonIndex(inputTableName);
			if(index != -1) {
				Map commonTable = null;

				//Map tableInfo = null;
				for(int j=0; j<this.commonTableList.size(); j++) {
					commonTable = (Map) this.commonTableList.get(j);
					Integer sqlIndex = (Integer) commonTable.get("sqlIndex");
					if(sqlIndex.intValue() == index) break;
				}
				
				List columnList = (List) commonTable.get("columnNameList");
	
				for (int j = 0; j < columnList.size(); j++) {
					String columnName = (String) columnList.get(j);
					if(columnName.equalsIgnoreCase(col_name)) {
						Map sourceColumn = new HashMap();
						sourceColumn.put("tableName", inputTableName);
						sourceColumn.put("columnName", columnName);
						sourceColumnList.add(sourceColumn);
					}
				}
			}

		}
		return new ArrayList(new HashSet(sourceColumnList));
	}
	
	/**
	 * 取得直接的SourceColumn列表
	 */
	private List getDirectSourceColumnList(String qualifyName, String columnExpStr, TableSelectExpression select) 
		throws MDSException {
		if(qualifyName.equalsIgnoreCase("A3.AGT_AMT_077")) {
			System.out.println(qualifyName);
		}
		List sourceColumnList = new ArrayList();
		HashMap sourceColumn = new HashMap();
		String temp[] = null;
		if(qualifyName.indexOf("_constant_") == 0) {
			temp = new String[1];
			temp[0] = qualifyName;
		} else temp = qualifyName.split("\\.");

		// column format is database.table.column
		if (temp.length == 3) {
			String databaseName = temp[0].trim();
			String tableName = temp[1].trim();
			String columnName = temp[2].trim();
			sourceColumn.put("databaseName", databaseName);
			sourceColumn.put("tableName", tableName);
			sourceColumn.put("columnName", columnName);
			sourceColumn.put("expression", columnExpStr);
			sourceColumnList.add(sourceColumn);
			return sourceColumnList;
		}
		// column format is : table.column
		else if (temp.length == 2) {
			// get the related table name list in from clause
			List tableNameList = getTablesInFrom(select);
			// related table alias name list in from clause
			List tableAliasList = getTableAliasInFrom(select);
			String tName = temp[0].trim();

			for (int i = 0; i < tableNameList.size(); i++) {
				String tableName = (String) tableNameList.get(i);
				String aliasName = (String) tableAliasList.get(i);
				// the prefix is a volatile name
				if (tName.equalsIgnoreCase(tableName) && !tableName.equalsIgnoreCase(aliasName)) {
					//FIXME:需要修改下面两个对于getSourceColumnList的调用
					if (!"".equalsIgnoreCase(fileName)) {
						qualifyName = fileName.trim() + "." + qualifyName;
						sourceColumnList = getDirectSourceColumnList(qualifyName, columnExpStr, select);
					} else
						sourceColumnList = getDirectSourceColumnList(qualifyName, columnExpStr, tableName, true);
					return sourceColumnList;
				} else if (tableName.equalsIgnoreCase(aliasName) && tName.equalsIgnoreCase(tableName)) {
					// the prefix is a subquery table name
					TableSelectExpression subQueryExp = getTempTableDefinition(aliasName, select);
					sourceColumnList = getDirectSourceColumnList(temp[1].trim(), columnExpStr, subQueryExp);
					// 处理union语句
					if (subQueryExp != null) {
						while (subQueryExp.composite_function != -1) {
							subQueryExp = subQueryExp.next_composite;
							List sourceColumnsIncomposite = getDirectSourceColumnList(temp[1].trim(), columnExpStr,
									subQueryExp);
							sourceColumnList.addAll(sourceColumnsIncomposite);

							// for (int z = 0; z < sourceColumnsIncomposite.size(); z++) {
							// HashMap s_c = (HashMap) sourceColumnsIncomposite.get(z);
							// sourceColumnList.add(s_c);
							// }
							sourceColumnsIncomposite = null;
						}
					}
					return sourceColumnList;
				}
				// the prefix is an alias name
				else if (tName.equalsIgnoreCase(aliasName)) {
					String sourceTable = getSourceTable(aliasName, select);
					// whether the sourceTable is a volatile temp table
					int index = -1;
					index = volatileIndex(sourceTable);
					// if a volatile table
					if (index >= 0) {
						sourceColumn.put("databaseName", "");
						sourceColumn.put("tableName", sourceTable);
						sourceColumn.put("columnName", temp[1].trim());
						sourceColumn.put("expression", columnExpStr);
						sourceColumnList.add(sourceColumn);
						return sourceColumnList;
					} else {
						// if not an volatile table, and is a temp table
						index = commonIndex(sourceTable);
						// if a temp table
						if (index >= 0) {
							sourceColumn.put("databaseName", "");
							sourceColumn.put("tableName", sourceTable);
							sourceColumn.put("columnName", temp[1].trim());
							sourceColumn.put("expression", columnExpStr);
							sourceColumnList.add(sourceColumn);
							return sourceColumnList;
						} else {
							String[] sourceTable_array = sourceTable.split("\\.");
							try {
								if (!"".equalsIgnoreCase(fileName)) {
									if (sourceTable_array.length == 1) {
										sourceTable = fileName.trim() + "." + sourceTable;
										sourceTable_array = sourceTable.split("\\.");
									}
								}

								sourceColumn.put("databaseName", sourceTable_array[0].trim());
								sourceColumn.put("tableName", sourceTable_array[1].trim());
								sourceColumn.put("columnName", temp[1].trim());
								sourceColumn.put("expression", columnExpStr);
								sourceColumnList.add(sourceColumn);
								sourceColumn = null;
								return sourceColumnList;

							} catch (Exception e) {
								// logger.error(e.getMessage());
								throw new MDSException("table : " + sourceTable.split("\\.")[0]
										+ " without database reference!");
							}
						}
					}
				}
			}
		}
		// column format is : column; or the column is a constant
		else if (temp.length == 1) {
			if (qualifyName.indexOf("_constant_") == 0 || qualifyName.indexOf(":") >= 0) {
				sourceColumn.put("databaseName", "");
				sourceColumn.put("tableName", "");
				if (qualifyName.indexOf(":") >= 0)
					qualifyName = "_constant";
				sourceColumn.put("columnName", qualifyName);
				sourceColumn.put("expression", columnExpStr);
				sourceColumnList.add(sourceColumn);
				sourceColumn = null;
				return sourceColumnList;
			} else {
				// column name is an alias or a real column name with no prefixed with table name
				// get all columns in the select statement
				List columnInfoList = getColumnListInSelect(select);
				boolean isNotAlias = false;
				boolean hasAdded = false;
				boolean havePrefix = true;
				int compareNum = 1;
				for (int i = 0; i < columnInfoList.size(); i++) {
					HashMap columnInfo = (HashMap) columnInfoList.get(i);
					String column_Name = (String) columnInfo.get("columnName");
					String column_Alias = (String) columnInfo.get("columnAlias");
					String column_Exp = (String) columnInfo.get("columnExpression");
					String[] column_Name_array = column_Name.split("\\.");
					String column_table_name = "";
					String realColumnName = column_Name_array[0].trim();
					if (column_Name_array.length == 2) {
						column_table_name = column_Name_array[0].trim();
						realColumnName = column_Name_array[1].trim();
					} else if (column_Name_array.length == 3) {
						column_table_name = column_Name_array[0].trim() + "." + column_Name_array[1].trim();
						realColumnName = column_Name_array[2].trim();
					}
					// if ( (column_Name.equalsIgnoreCase(qualifyName) && column_Name_array.length==1) ||
					// (column_Name.toUpperCase().endsWith(qualifyName.toUpperCase()) && column_Name_array.length>1)) {
					if (realColumnName.equalsIgnoreCase(qualifyName)) {
						havePrefix = false;
						// decide in which table the column is in
						// get the related table name list in from clause
						List tableNameList = getTablesInFrom(select);
						// related table alias name list in from clause
						List tableAliasList = getTableAliasInFrom(select);

						for (int j = 0; j < tableNameList.size(); j++) {
							String tempName = (String) tableNameList.get(j);
							String aliasName = (String) tableAliasList.get(j);
							String[] tempName_array = tempName.split("\\.");

							if (!"".equalsIgnoreCase(fileName)) {
								if (tempName_array.length == 1 && !tempName.equalsIgnoreCase(aliasName)) {
									tempName = fileName.trim() + "." + tempName;
									tempName_array = tempName.split("\\.");
								}
							}
							if (!"".equalsIgnoreCase(column_table_name)) {
								if (!column_table_name.equalsIgnoreCase(tempName)
										&& !column_table_name.equalsIgnoreCase(aliasName))
									continue;
							}
							if (tempName_array.length < 2) {
								// if the column is in a volatile table
								if (!aliasName.equalsIgnoreCase(tempName)
										&& columnInVolatileTable(qualifyName, tempName)) {
									isNotAlias = true;
									
									Map column = new HashMap();
									column.put("databaseName", "");
									column.put("tableName", tempName);
									column.put("columnName", qualifyName);
									column.put("expression", columnExpStr);
									sourceColumnList.add(column);
									return sourceColumnList;
								}
							}
							// if ( ! "".equalsIgnoreCase(fileName)) {
							// if ( tempName_array.length == 1 && ! tempName.equalsIgnoreCase(aliasName))
							// tempName = fileName.trim() + "." + tempName;
							// }
							// really table in select statement
							if (tempName_array.length == 2) {
								String dbName = tempName_array[0].trim();
								String tName = tempName_array[1].trim();
								if ((column_Name.toUpperCase().indexOf(qualifyName.toUpperCase()) >= 0)
										&& (columnInTable(qualifyName, tName, dbName))) {
									isNotAlias = true;
									// if the table is a temp table (created in the log file)
									int commonIndex = commonIndex(dbName + "." + tName);
									if (commonIndex > -1) {
										sourceColumnList = getSourceColumnList(
												dbName + "." + tName + "." + qualifyName, columnExpStr + "==>"
														+ qualifyName, dbName + "." + tName, false);	//FIXME:修改对于getSourceColumnList的调用
									} else {
										sourceColumn.put("databaseName", tempName_array[0].trim());
										sourceColumn.put("tableName", tempName_array[1].trim());
										sourceColumn.put("columnName", qualifyName);
										if (column_Exp.indexOf(column_Name) < 0)
											columnExpStr = column_Exp + " AS " + column_Alias + "  ==>  "
													+ columnExpStr;
										sourceColumn.put("expression", columnExpStr);
										sourceColumnList.add(sourceColumn);
										sourceColumn = null;
									}
									return sourceColumnList;
								}
							}
							/*
							 * // if the column is in a volatile table else if ( ! aliasName.equalsIgnoreCase(tempName)
							 * && columnInVolatileTable(qualifyName,tempName) ) { isNotAlias = true; //else if (
							 * columnInVolatileTable(qualifyName,tempName) ) { sourceColumnList =
							 * getSourceColumnList(tempName + "." + qualifyName, columnExpStr,tempName,true); return
							 * sourceColumnList; }
							 */
							// subquery table in select statement
							else {
								TableSelectExpression subQueryExp = getTempTableDefinition(tempName, select);
								if (subQueryExp != null) {
									ArrayList aliasList = subQueryExp.aliasList;
									// boolean inSubquery = false;
									List columnList = getColumnListInSelect(subQueryExp);
									hasAdded = false;
									for (int z = 0; z < columnList.size(); z++) {
										// Boolean hasAdded = false;
										HashMap column = (HashMap) columnList.get(z);
										String columnName = (String) column.get("columnName");
										String columnAliasName = (String) column.get("columnAlias");
										String columnExpression = (String) column.get("columnExpression");
										String[] columnNameArr = columnName.split("\\.");
										String realColumnName1 = columnNameArr[0].trim();
										if (columnNameArr.length == 2)
											realColumnName1 = columnNameArr[1].trim();
										else if (columnNameArr.length == 3)
											realColumnName1 = columnNameArr[2].trim();
										// if
										// (qualifyName.equalsIgnoreCase(columnName)||columnName.toUpperCase().endsWith(qualifyName.toUpperCase()))
										// {
										if (qualifyName.equalsIgnoreCase(realColumnName1)) {
											if (!hasAdded) {
												columnExpStr = columnExpStr;// + "<==" + columnExpression;
												hasAdded = true;
											}
											sourceColumnList = getDirectSourceColumnList(columnName, columnExpStr,
													subQueryExp);

										} else if (qualifyName.equalsIgnoreCase(columnAliasName)) {
											List sColumnList = getDirectSourceColumnList(columnName, columnExpStr,
													subQueryExp);
											sourceColumnList.addAll(sColumnList);

											// for (int t = 0; t < sColumnList.size(); t++) {
											// HashMap s_c = (HashMap) sColumnList.get(t);
											// sourceColumnList.add(s_c);
											// }
											sColumnList = null;
										}
									}
									while (subQueryExp.composite_function != -1) {
										subQueryExp = subQueryExp.next_composite;
										subQueryExp.aliasList = aliasList;
										columnList = getColumnListInSelect(subQueryExp);
										hasAdded = false;
										for (int z = 0; z < columnList.size(); z++) {
											HashMap column = (HashMap) columnList.get(z);
											String columnName = (String) column.get("columnName");
											String columnAliasName = (String) column.get("columnAlias");
											String columnExpression = (String) column.get("columnExpression");
											List sColumnList = new ArrayList();
											if (qualifyName.equalsIgnoreCase(columnName)) {
												if (!hasAdded) {
													columnExpStr = columnExpStr;// + "<==" + columnExpression;
													hasAdded = true;
												}
												sColumnList = getDirectSourceColumnList(columnName, columnExpStr, subQueryExp);
											} else if (qualifyName.equalsIgnoreCase(columnAliasName)) {
												sColumnList = getDirectSourceColumnList(columnName, columnExpStr, subQueryExp);
											}
											sourceColumnList.addAll(sColumnList);

											// for (int t = 0; t < sColumnList.size(); t++) {
											// HashMap s_c = (HashMap) sColumnList.get(t);
											// sourceColumnList.add(s_c);
											// }
											sColumnList = null;
										}
									}
									return sourceColumnList;
								}
							}
						}
					}
					compareNum++;
				}
				if (!isNotAlias) {
					hasAdded = false;
					for (int i = 0; i < columnInfoList.size(); i++) {
						HashMap columnInfo = (HashMap) columnInfoList.get(i);
						String column_Name = (String) columnInfo.get("columnName");
						String column_Alias = (String) columnInfo.get("columnAlias");
						String column_Exp = (String) columnInfo.get("columnExpression");
						// if the qualify name is alias name
						if (qualifyName.equalsIgnoreCase(column_Alias)
								&& (!column_Name.equalsIgnoreCase(column_Alias)
										|| column_Name.equalsIgnoreCase("_constant") || column_Name.indexOf(":") > 0)) {
							if (!hasAdded) {
								columnExpStr = column_Exp + " AS " + column_Alias + "  ==>  " + columnExpStr;
								hasAdded = true;
							}
							List sColumnList = getDirectSourceColumnList(column_Name, columnExpStr, select);
							sourceColumnList.addAll(sColumnList);

							// for (int j = 0; j < sColumnList.size(); j++) {
							// HashMap s_c = (HashMap) sColumnList.get(j);
							// sourceColumnList.add(s_c);
							// }
							sColumnList = null;

						}
					}
					return sourceColumnList;
				}
				// column name is not an alias, and has prefix
				if (havePrefix) {
					for (int i = 0; i < columnInfoList.size(); i++) {
						// Boolean hasAdded = false;
						HashMap columnInfo = (HashMap) columnInfoList.get(i);
						String column_Name = (String) columnInfo.get("columnName");
						String column_Alias = (String) columnInfo.get("columnAlias");
						String column_Exp = (String) columnInfo.get("columnExpression");
						String[] column_Name_array = column_Name.split("\\.");

						if ((!column_Name.equalsIgnoreCase("_constant") || column_Name.indexOf(":") > 0)
								&& column_Name_array.length > 1) {
							String nameWithNoPrefix = column_Name_array[1].trim();
							if (nameWithNoPrefix.equalsIgnoreCase(qualifyName)) {
								if (!hasAdded) {
									columnExpStr = column_Exp + "  ==>  " + columnExpStr;
									hasAdded = true;
								}
								sourceColumnList = getDirectSourceColumnList(column_Name, columnExpStr, select);
								return sourceColumnList;
							}
						}
					}
				}
				if (compareNum > columnInfoList.size())
					// throw new MDSException("column : " + qualifyName + " is not a column of any tables!" );
					logger.error("Warning:  column : " + qualifyName + " is not a column of any tables!");
				//return new ArrayList(new HashSet(sourceColumnList));
				return sourceColumnList;
			}
		}
		//return new ArrayList(new HashSet(sourceColumnList));
		return sourceColumnList;
	}

	/**
	 * get source column list according to given column name, column expression, select statement; This function is used
	 * to find source columns of columns.
	 * 
	 * @param qualifyName
	 *            ;columnExpStr,inputTableName,isVolatile.
	 * @return
	 * @throws MDSException
	 */
	public List getSourceColumnList(String qualifyName, String columnExpStr, TableSelectExpression select)
			throws MDSException {
		List sourceColumnList = new ArrayList();
		HashMap sourceColumn = new HashMap();
		String temp[] = null;
		if(qualifyName.indexOf("_constant_") == 0) {
			temp = new String[1];
			temp[0] = qualifyName;
		} else temp = qualifyName.split("\\.");

		// column format is database.table.column
		if (temp.length == 3) {
			String databaseName = temp[0].trim();
			String tableName = temp[1].trim();
			String columnName = temp[2].trim();
			// if the table is a temp table (created in the log file)
			int commonIndex = commonIndex(databaseName + "." + tableName);
			if (commonIndex > -1) {
				sourceColumnList = getSourceColumnList(databaseName + "." + tableName + "." + columnName, columnExpStr
						+ "==>" + columnName, databaseName + "." + tableName, false);
			} else {
				sourceColumn.put("databaseName", databaseName);
				sourceColumn.put("tableName", tableName);
				sourceColumn.put("columnName", columnName);
				sourceColumn.put("expression", columnExpStr);
				sourceColumnList.add(sourceColumn);
			}
			return sourceColumnList;
		}
		// column format is : table.column
		else if (temp.length == 2) {
			// get the related table name list in from clause
			List tableNameList = getTablesInFrom(select);
			// related table alias name list in from clause
			List tableAliasList = getTableAliasInFrom(select);
			String tName = temp[0].trim();

			for (int i = 0; i < tableNameList.size(); i++) {
				String tableName = (String) tableNameList.get(i);
				String aliasName = (String) tableAliasList.get(i);
				// the prefix is a volatile name
				if (tName.equalsIgnoreCase(tableName) && !tableName.equalsIgnoreCase(aliasName)) {
					if (!"".equalsIgnoreCase(fileName)) {
						qualifyName = fileName.trim() + "." + qualifyName;
						sourceColumnList = getSourceColumnList(qualifyName, columnExpStr, select);
					} else
						sourceColumnList = getSourceColumnList(qualifyName, columnExpStr, tableName, true);
					return sourceColumnList;
				} else if (tableName.equalsIgnoreCase(aliasName) && tName.equalsIgnoreCase(tableName)) {
					// the prefix is a subquery table name
					TableSelectExpression subQueryExp = getTempTableDefinition(aliasName, select);
					sourceColumnList = getSourceColumnList(temp[1].trim(), columnExpStr, subQueryExp);
					// 处理union语句
					if (subQueryExp != null) {
						while (subQueryExp.composite_function != -1) {
							subQueryExp = subQueryExp.next_composite;
							List sourceColumnsIncomposite = getSourceColumnList(temp[1].trim(), columnExpStr,
									subQueryExp);
							sourceColumnList.addAll(sourceColumnsIncomposite);

							// for (int z = 0; z < sourceColumnsIncomposite.size(); z++) {
							// HashMap s_c = (HashMap) sourceColumnsIncomposite.get(z);
							// sourceColumnList.add(s_c);
							// }
							sourceColumnsIncomposite = null;
						}
					}
					return sourceColumnList;
				}
				// the prefix is an alias name
				else if (tName.equalsIgnoreCase(aliasName)) {
					String sourceTable = getSourceTable(aliasName, select);
					// whether the sourceTable is a volatile temp table
					int index = -1;
					index = volatileIndex(sourceTable);
					// if a volatile table
					if (index >= 0) {
						sourceColumnList = getSourceColumnList(sourceTable + "." + temp[1].trim(), columnExpStr,
								sourceTable, true);
						return sourceColumnList;
					} else {
						// if not an volatile table, and is a temp table
						index = commonIndex(sourceTable);
						// if a temp table
						if (index >= 0) {
							sourceColumnList = getSourceColumnList(sourceTable + "." + temp[1].trim(), columnExpStr,
									sourceTable, false);
							return sourceColumnList;
						} else {
							String[] sourceTable_array = sourceTable.split("\\.");
							try {
								if (!"".equalsIgnoreCase(fileName)) {
									if (sourceTable_array.length == 1) {
										sourceTable = fileName.trim() + "." + sourceTable;
										sourceTable_array = sourceTable.split("\\.");
									}
								}

								sourceColumn.put("databaseName", sourceTable_array[0].trim());
								sourceColumn.put("tableName", sourceTable_array[1].trim());
								sourceColumn.put("columnName", temp[1].trim());
								sourceColumn.put("expression", columnExpStr);
								sourceColumnList.add(sourceColumn);
								sourceColumn = null;
								return sourceColumnList;

							} catch (Exception e) {
								// logger.error(e.getMessage());
								throw new MDSException("table : " + sourceTable.split("\\.")[0]
										+ " without database reference!");
							}
						}
					}
				}
			}
		}
		// column format is : column; or the column is a constant
		else if (temp.length == 1) {
			if (qualifyName.indexOf("_constant_") == 0 || qualifyName.indexOf(":") >= 0) {
				sourceColumn.put("databaseName", "");
				sourceColumn.put("tableName", "");
				if (qualifyName.indexOf(":") >= 0)
					qualifyName = "_constant";
				sourceColumn.put("columnName", qualifyName);
				sourceColumn.put("expression", columnExpStr);
				sourceColumnList.add(sourceColumn);
				sourceColumn = null;
				return sourceColumnList;
			} else {
				// column name is an alias or a real column name with no prefixed with table name
				// get all columns in the select statement
				List columnInfoList = getColumnListInSelect(select);
				boolean isNotAlias = false;
				boolean hasAdded = false;
				boolean havePrefix = true;
				int compareNum = 1;
				for (int i = 0; i < columnInfoList.size(); i++) {
					HashMap columnInfo = (HashMap) columnInfoList.get(i);
					String column_Name = (String) columnInfo.get("columnName");
					String column_Alias = (String) columnInfo.get("columnAlias");
					String column_Exp = (String) columnInfo.get("columnExpression");
					String[] column_Name_array = column_Name.split("\\.");
					String column_table_name = "";
					String realColumnName = column_Name_array[0].trim();
					if (column_Name_array.length == 2) {
						column_table_name = column_Name_array[0].trim();
						realColumnName = column_Name_array[1].trim();
					} else if (column_Name_array.length == 3) {
						column_table_name = column_Name_array[0].trim() + "." + column_Name_array[1].trim();
						realColumnName = column_Name_array[2].trim();
					}
					// if ( (column_Name.equalsIgnoreCase(qualifyName) && column_Name_array.length==1) ||
					// (column_Name.toUpperCase().endsWith(qualifyName.toUpperCase()) && column_Name_array.length>1)) {
					if (realColumnName.equalsIgnoreCase(qualifyName)) {
						havePrefix = false;
						// decide in which table the column is in
						// get the related table name list in from clause
						List tableNameList = getTablesInFrom(select);
						// related table alias name list in from clause
						List tableAliasList = getTableAliasInFrom(select);

						for (int j = 0; j < tableNameList.size(); j++) {
							String tempName = (String) tableNameList.get(j);
							String aliasName = (String) tableAliasList.get(j);
							String[] tempName_array = tempName.split("\\.");

							if (!"".equalsIgnoreCase(fileName)) {
								if (tempName_array.length == 1 && !tempName.equalsIgnoreCase(aliasName)) {
									tempName = fileName.trim() + "." + tempName;
									tempName_array = tempName.split("\\.");
								}
							}
							if (!"".equalsIgnoreCase(column_table_name)) {
								if (!column_table_name.equalsIgnoreCase(tempName)
										&& !column_table_name.equalsIgnoreCase(aliasName))
									continue;
							}
							if (tempName_array.length < 2) {
								// if the column is in a volatile table
								if (!aliasName.equalsIgnoreCase(tempName)
										&& columnInVolatileTable(qualifyName, tempName)) {
									isNotAlias = true;
									// else if ( columnInVolatileTable(qualifyName,tempName) ) {
									sourceColumnList = getSourceColumnList(tempName + "." + qualifyName, columnExpStr,
											tempName, true);
									return sourceColumnList;
								}
							}
							// if ( ! "".equalsIgnoreCase(fileName)) {
							// if ( tempName_array.length == 1 && ! tempName.equalsIgnoreCase(aliasName))
							// tempName = fileName.trim() + "." + tempName;
							// }
							// really table in select statement
							if (tempName_array.length == 2) {
								String dbName = tempName_array[0].trim();
								String tName = tempName_array[1].trim();
								if ((column_Name.toUpperCase().indexOf(qualifyName.toUpperCase()) >= 0)
										&& (columnInTable(qualifyName, tName, dbName))) {
									isNotAlias = true;
									// if the table is a temp table (created in the log file)
									int commonIndex = commonIndex(dbName + "." + tName);
									if (commonIndex > -1) {
										sourceColumnList = getSourceColumnList(
												dbName + "." + tName + "." + qualifyName, columnExpStr + "==>"
														+ qualifyName, dbName + "." + tName, false);
									} else {
										sourceColumn.put("databaseName", tempName_array[0].trim());
										sourceColumn.put("tableName", tempName_array[1].trim());
										sourceColumn.put("columnName", qualifyName);
										if (column_Exp.indexOf(column_Name) < 0)
											columnExpStr = column_Exp + " AS " + column_Alias + "  ==>  "
													+ columnExpStr;
										sourceColumn.put("expression", columnExpStr);
										sourceColumnList.add(sourceColumn);
										sourceColumn = null;
									}
									return sourceColumnList;
								}
							}
							/*
							 * // if the column is in a volatile table else if ( ! aliasName.equalsIgnoreCase(tempName)
							 * && columnInVolatileTable(qualifyName,tempName) ) { isNotAlias = true; //else if (
							 * columnInVolatileTable(qualifyName,tempName) ) { sourceColumnList =
							 * getSourceColumnList(tempName + "." + qualifyName, columnExpStr,tempName,true); return
							 * sourceColumnList; }
							 */
							// subquery table in select statement
							else {
								TableSelectExpression subQueryExp = getTempTableDefinition(tempName, select);
								if (subQueryExp != null) {
									ArrayList aliasList = subQueryExp.aliasList;
									// boolean inSubquery = false;
									List columnList = getColumnListInSelect(subQueryExp);
									hasAdded = false;
									for (int z = 0; z < columnList.size(); z++) {
										// Boolean hasAdded = false;
										HashMap column = (HashMap) columnList.get(z);
										String columnName = (String) column.get("columnName");
										String columnAliasName = (String) column.get("columnAlias");
										String columnExpression = (String) column.get("columnExpression");
										String[] columnNameArr = columnName.split("\\.");
										String realColumnName1 = columnNameArr[0].trim();
										if (columnNameArr.length == 2)
											realColumnName1 = columnNameArr[1].trim();
										else if (columnNameArr.length == 3)
											realColumnName1 = columnNameArr[2].trim();
										// if
										// (qualifyName.equalsIgnoreCase(columnName)||columnName.toUpperCase().endsWith(qualifyName.toUpperCase()))
										// {
										if (qualifyName.equalsIgnoreCase(realColumnName1)) {
											if (!hasAdded) {
												columnExpStr = columnExpStr;// + "<==" + columnExpression;
												hasAdded = true;
											}
											sourceColumnList = getSourceColumnList(columnName, columnExpStr,
													subQueryExp);

										} else if (qualifyName.equalsIgnoreCase(columnAliasName)) {
											List sColumnList = getSourceColumnList(columnName, columnExpStr,
													subQueryExp);
											sourceColumnList.addAll(sColumnList);

											// for (int t = 0; t < sColumnList.size(); t++) {
											// HashMap s_c = (HashMap) sColumnList.get(t);
											// sourceColumnList.add(s_c);
											// }
											sColumnList = null;
										}
									}
									while (subQueryExp.composite_function != -1) {
										subQueryExp = subQueryExp.next_composite;
										subQueryExp.aliasList = aliasList;
										columnList = getColumnListInSelect(subQueryExp);
										hasAdded = false;
										for (int z = 0; z < columnList.size(); z++) {
											HashMap column = (HashMap) columnList.get(z);
											String columnName = (String) column.get("columnName");
											String columnAliasName = (String) column.get("columnAlias");
											String columnExpression = (String) column.get("columnExpression");
											List sColumnList = new ArrayList();
											if (qualifyName.equalsIgnoreCase(columnName)) {
												if (!hasAdded) {
													columnExpStr = columnExpStr;// + "<==" + columnExpression;
													hasAdded = true;
												}
												sColumnList = getSourceColumnList(columnName, columnExpStr, subQueryExp);
											} else if (qualifyName.equalsIgnoreCase(columnAliasName)) {
												sColumnList = getSourceColumnList(columnName, columnExpStr, subQueryExp);
											}
											sourceColumnList.addAll(sColumnList);

											// for (int t = 0; t < sColumnList.size(); t++) {
											// HashMap s_c = (HashMap) sColumnList.get(t);
											// sourceColumnList.add(s_c);
											// }
											sColumnList = null;
										}
									}
									return sourceColumnList;
								}
							}
						}
					}
					compareNum++;
				}
				if (!isNotAlias) {
					hasAdded = false;
					for (int i = 0; i < columnInfoList.size(); i++) {
						HashMap columnInfo = (HashMap) columnInfoList.get(i);
						String column_Name = (String) columnInfo.get("columnName");
						String column_Alias = (String) columnInfo.get("columnAlias");
						String column_Exp = (String) columnInfo.get("columnExpression");
						// if the qualify name is alias name
						if (qualifyName.equalsIgnoreCase(column_Alias)
								&& (!column_Name.equalsIgnoreCase(column_Alias)
										|| column_Name.equalsIgnoreCase("_constant") || column_Name.indexOf(":") > 0)) {
							if (!hasAdded) {
								columnExpStr = column_Exp + " AS " + column_Alias + "  ==>  " + columnExpStr;
								hasAdded = true;
							}
							List sColumnList = getSourceColumnList(column_Name, columnExpStr, select);
							sourceColumnList.addAll(sColumnList);

							// for (int j = 0; j < sColumnList.size(); j++) {
							// HashMap s_c = (HashMap) sColumnList.get(j);
							// sourceColumnList.add(s_c);
							// }
							sColumnList = null;

						}
					}
					return sourceColumnList;
				}
				// column name is not an alias, and has prefix
				if (havePrefix) {
					for (int i = 0; i < columnInfoList.size(); i++) {
						// Boolean hasAdded = false;
						HashMap columnInfo = (HashMap) columnInfoList.get(i);
						String column_Name = (String) columnInfo.get("columnName");
						String column_Alias = (String) columnInfo.get("columnAlias");
						String column_Exp = (String) columnInfo.get("columnExpression");
						String[] column_Name_array = column_Name.split("\\.");

						if ((!column_Name.equalsIgnoreCase("_constant") || column_Name.indexOf(":") > 0)
								&& column_Name_array.length > 1) {
							String nameWithNoPrefix = column_Name_array[1].trim();
							if (nameWithNoPrefix.equalsIgnoreCase(qualifyName)) {
								if (!hasAdded) {
									columnExpStr = column_Exp + "  ==>  " + columnExpStr;
									hasAdded = true;
								}
								sourceColumnList = getSourceColumnList(column_Name, columnExpStr, select);
								return sourceColumnList;
							}
						}
					}
				}
				if (compareNum > columnInfoList.size())
					// throw new MDSException("column : " + qualifyName + " is not a column of any tables!" );
					logger.error("Warning:  column : " + qualifyName + " is not a column of any tables!");
				//return new ArrayList(new HashSet(sourceColumnList));
				return sourceColumnList;
			}
		}
		//return new ArrayList(new HashSet(sourceColumnList));
		return sourceColumnList;
	}

	/**
	 * get table name according to a given qualified name; here, table name may be prefixed with database name
	 * 
	 * @param columnName
	 *            column name.
	 * @return
	 */
	public String getTalbeName(String columnName) {
		String tableName = "";
		String[] temp = columnName.split("\\.");
		if (temp.length == 2) {
			tableName = temp[0].trim();
		} else if (temp.length == 3) {
			tableName = temp[0].trim() + "." + temp[1].trim();
		}
		return tableName;
	}

	/**
	 * insert the relationship of two columns into TAP_C_META_UNIT_RELATION;
	 * 
	 * @param
	 * @return
	 * 
	 * Modified by MarkDong
	 */

	public void addRelation(String Src_Obj_ID
						  , String Tgt_Obj_ID
						  , String Data_Rela_Modify
						  , String Data_Rela_Cd
						  , String Auto_Type_Cd
						  , String Data_Rela_Src_ID
						  , String Rela_Desc
						  , String Data_Rela_Src_Name
						  , String Const_Cd_Col_id
						  , String Const_Cd_Value) {
//		String comment = Rela_Desc.replaceAll("'", "''");
//		String sql = "insert into " + this.tempDB + "MT02_DATA_RELA_C " + " values ('" + Src_Obj_ID + "','"
//				+ Tgt_Obj_ID + "','" + Data_Rela_Modify + "'," + Data_Rela_Cd + "," + Auto_Type_Cd + ",'"
//				+ Data_Rela_Src_ID + "','" + comment + "')";
		
		
		//FIXME: 表字段Data_Rela_Scr_ID和Data_Rela_Src_Name有些混乱
		int data_rela_cd = Integer.parseInt(Data_Rela_Cd);
		int auto_type_cd = Integer.parseInt(Auto_Type_Cd);
		DataRelaColumnVO relaCVO = new DataRelaColumnVO(Src_Obj_ID
													, Tgt_Obj_ID
													, Data_Rela_Modify
													, data_rela_cd
													, auto_type_cd
													, Data_Rela_Src_ID
													, Rela_Desc.replaceAll("'", "''")
													, Data_Rela_Src_Name
													, Const_Cd_Col_id
													, Const_Cd_Value);
		
		insertArray.add(relaCVO);
	}

	/**
	 * decide whether a column is in a table;
	 * 
	 * @param columnName
	 *            column name; tableName table name;sysName database name.
	 * @return if a column in a given table, return "true". else "false"
	 * @throws MDSException
	 */
	public boolean columnInTable(String columnName, String tableName, String sysName) throws MDSException {
		boolean inTable = false;
		// if the table is created in the script log file (a temp table)
		List columns_List = (ArrayList) getAllColumns(sysName, tableName);
		for (int i = 0; i < columns_List.size(); i++) {
			SelectColumn column = (SelectColumn) columns_List.get(i);
			if (column.expression.text().toString().equalsIgnoreCase(columnName)) {
				inTable = true;
				break;
			}
		}
		return inTable;
	}

	/**
	 * decide whether a column is in a volatiole table;
	 * 
	 * @param columnName
	 *            column name; tableName table name.
	 * @return if a column in the given volatile table, return "true". else "false"
	 * @throws MDSException
	 */
	public boolean columnInVolatileTable(String columnName, String tableName) throws MDSException {
		boolean inTable = false;
		List columnList = getAllColumns("", tableName);
		for (int i = 0; i < columnList.size(); i++) {
			SelectColumn selColumn = (SelectColumn) columnList.get(i);
			String cName = selColumn.expression.text().toString();
			;
			if (columnName.equalsIgnoreCase(cName)) {
				inTable = true;
				break;
			}
		}
		return inTable;
	}

	/**
	 * get column expression according alias name and select statement;
	 * 
	 * @param aliasName
	 *            ; select expression.
	 * @return
	 */
	public Expression getAliasExpression(String aliasName, TableSelectExpression selExp) {
		Expression exp = null;
		List columns = selExp.columns;

		for (int i = 0; i < columns.size(); i++) {
			SelectColumn column = (SelectColumn) columns.get(i);
			if (column.alias.equalsIgnoreCase(aliasName)) {
				exp = column.expression;
				break;
			}
		}
		return exp;
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

	public String getWhereStr(String sql) throws MDSException {
		SQL parser = new SQL(getStream(sql));
		StatementTree statement;
		String whereStr = null;

		try {
			statement = parser.Statement();

			HashMap selmap = statement.map;
			TableSelectExpression selExp = (TableSelectExpression) selmap.get("table_expression");
			// where clause in select statement
			SearchExpression where = (SearchExpression) selExp.where_clause;
			if (where != null) {
				Expression whereExp = where.getFromExpression();
				if (whereExp != null) {
					whereStr = whereExp.text().toString();
				}
			}
		} catch (Throwable e) {
			logger.error(e.getMessage());
			throw new MDSException(e);
		}
		// System.out.println(whereStr);
		return whereStr;
	}

	public List getColumnsInWhere(StatementTree select) {
		List columnNameList = new ArrayList();
		HashMap selmap = select.map;
		TableSelectExpression selExp = (TableSelectExpression) selmap.get("table_expression");
		// where clause in select statement
		SearchExpression where = (SearchExpression) selExp.where_clause;
		Expression whereExp = where.getFromExpression();
		if (whereExp != null) {
			for (int i = 0; i < whereExp.allVariables().size(); i++) {
				// varialbes in where clause
				Variable var = (Variable) whereExp.allVariables().get(i);
				if (var.getTableName() != null) {
					String columnName = var.table_name.toString() + "." + var.getName();
					columnNameList.add(columnName);
				} else
					columnNameList.add(var.getName());
			}
		}
		return columnNameList;
	}

	/**
	 * read a sql log file to db and parse it
	 * 
	 * @param sqlList
	 *            sqls extracted from a file. fileName the file to be read.
	 */
	public List bteqBridge(List sqlList, String fileName) throws MDSException {
		sqlNum = sqlList.size();

//		PropertiesLoader bootProps;
//		try {
//			bootProps = PropertiesLoader.getLoader("tap.properties");
//			if (bootProps != null) {
//				String limit = bootProps.getProperty("SQL_NUM_LIMIT");
//				if (limit != null && limit.length() > 0)
//					sql_num_limit = Integer.parseInt(limit);
//				else
//					sql_num_limit = 100000;
//
//			}
//		} catch (Exception e) {
//			// e.printStackTrace();
//		}
		sql_num_limit = 100000;		//modified by MarkDong
		
		int pos = fileName.indexOf(".log");
		if (pos > 0) 
			fileName = fileName.substring(0, pos);
		String scriptID = getScriptID(fileName);
		
		for (int i = 0; i < sqlList.size(); i++) {
			List sourceColumns = new ArrayList();
			sqlIndex = i;

			String sql = (String) sqlList.get(i);
			SQL parser = new SQL(getStream(sql.toString()));

			StatementTree statement;
			try {
				statement = parser.Statement();

				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.Select")) {
					parseSelect(statement);
					// getWhereStr(statement);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.Delete")) {
					parseDelete(statement);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.Insert")) {
					// long parseStartTime = System.currentTimeMillis();
					// logger.info("-------sql:"+sql);
					sourceColumns = parseInsert(statement);
					// long parseEndTime = System.currentTimeMillis();
					// logger.info("-------parser time=>"+(parseEndTime - parseStartTime));
					if (sourceColumns == null && blankComment) {
						return null;
					}
					// parseStartTime = System.currentTimeMillis();
					for (int z = 0; z < sourceColumns.size(); z++) {
						HashMap resultColumn = (HashMap) sourceColumns.get(z);
						String targetDBName = (String) resultColumn.get("targetDBName");
						String targetTName = (String) resultColumn.get("targetTName");
						String targetCName = (String) resultColumn.get("targetCName");
						MDSDbVO targetUnit = getUnitDB(targetDBName, targetTName, targetCName);
						// System.out.println(targetUnit);
						String sourceDBName = (String) resultColumn.get("sourceDBName");
						String sourceTName = (String) resultColumn.get("sourceTName");
						String sourceCName = (String) resultColumn.get("sourceCName");

						String Tgt_Obj_ID = targetUnit.getMETA_GLOBAL_ID();

						String commentID = (String) resultColumn.get("expression");
						String relationComm = commentID;
						if (sqlNum > sql_num_limit)
							relationComm = getComment(commentID);

						String Src_Obj_ID = "0";

						if (sourceCName != null && sourceCName.indexOf("_constant") != 0) {
							MDSDbVO sourceUnit = getUnitDB(sourceDBName, sourceTName, sourceCName);
							if(sourceUnit != null)
								Src_Obj_ID = sourceUnit.getMETA_GLOBAL_ID();
						}

						if(Src_Obj_ID == null) {
							logger.warn(fileName + ":" + sql + ":" + "Src_Obj_ID is null");
							continue;
						}
						
						if(!Src_Obj_ID.equals("0")) {
							//FIXME:可以修改为不论有没有Conditions都插入一条不带常量条件的记录
							List conditions = (List) resultColumn.get("conditions");
							if(conditions == null || conditions.size() == 0) {
								addRelation(Src_Obj_ID
											, Tgt_Obj_ID
											, ""
											, "23"
											, "1"
											, scriptID
											, relationComm
											, fileName
											, ""
											, "");
							} else {
								for(int j=0; j<conditions.size(); j++) {
									Map condition = (Map) conditions.get(j);
									String database = (String) condition.get("database");
									String table = (String) condition.get("table");
									String column = (String) condition.get("column");
									String value = (String) condition.get("value");
									MDSDbVO constUnit = getUnitDB(database, table, column);
									String Const_Cd_Col_ID = "";
									try {
										Const_Cd_Col_ID = constUnit.getMETA_GLOBAL_ID();
									} catch(Exception e) {
										logger.warn(fileName + ":精确解析发生错误，没找到定义的关系字段" );
									}
									addRelation(Src_Obj_ID
											, Tgt_Obj_ID
											, ""
											, "23"
											, "1"
											, scriptID
											, relationComm
											, fileName
											, Const_Cd_Col_ID
											, value);
								}
							}
						}
					}
					// parseEndTime = System.currentTimeMillis();
					// logger.info("------relation  =>"+(parseEndTime - parseStartTime));
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.ViewManager")) {
					sourceColumns = parseCreateView(statement, fileName);
					for (int z = 0; z < sourceColumns.size(); z++) {
						HashMap resultColumn = (HashMap) sourceColumns.get(z);
						String targetDBName = (String) resultColumn.get("targetDBName");
						String targetTName = (String) resultColumn.get("targetTName");
						String targetCName = (String) resultColumn.get("targetCName");
						MDSDbVO targetUnit = getUnitDB(targetDBName, targetTName, targetCName);
						// System.out.println(targetUnit);
						String sourceDBName = (String) resultColumn.get("sourceDBName");
						String sourceTName = (String) resultColumn.get("sourceTName");
						String sourceCName = (String) resultColumn.get("sourceCName");

						String Tgt_Obj_ID = targetUnit.getMETA_GLOBAL_ID();

						String commentID = (String) resultColumn.get("expression");
						String relationComm = commentID;
						if (sqlNum > sql_num_limit)
							relationComm = getComment(commentID);
						// String relationID = MetaRelationTypeConstants.Transform_View;

						String Src_Obj_ID = "0";

//						if (!"_constant".equalsIgnoreCase(sourceCName)) {
						if (sourceCName != null && sourceCName.indexOf("_constant") != 0) {

							MDSDbVO sourceUnit = getUnitDB(sourceDBName, sourceTName, sourceCName);
							Src_Obj_ID = sourceUnit.getMETA_GLOBAL_ID();
						}
						String relSrcID = getObjID(targetDBName, targetTName);
						//FIXME:需要处理
						List conditions = (List) resultColumn.get("conditions");
						if(conditions == null || conditions.size() == 0) {
							addRelation(Src_Obj_ID
										, Tgt_Obj_ID
										, ""
										, "23"
										, "1"
										, scriptID
										, relationComm
										, fileName
										, ""
										, "");
						} else {
							for(int j=0; j<conditions.size(); j++) {
								Map condition = (Map) conditions.get(j);
								String database = (String) condition.get("database");
								String table = (String) condition.get("table");
								String column = (String) condition.get("column");
								String value = (String) condition.get("value");
								MDSDbVO constUnit = getUnitDB(database, table, column);
								String Const_Cd_Col_ID = "";
								try {
									Const_Cd_Col_ID = constUnit.getMETA_GLOBAL_ID();
								} catch(Exception e) {
									logger.warn(fileName + "精确解析发生错误，没有找到常量字段");
								}
								addRelation(Src_Obj_ID
										, Tgt_Obj_ID
										, ""
										, "23"
										, "1"
										, scriptID
										, relationComm
										, fileName
										, Const_Cd_Col_ID
										, value);
							}
						}
					}
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.CreateTable")) {
					// get all volitale tables
					HashMap tableInfo = parseCreateTable(statement);
					
					//根据源表设置临时表的Is_Const_Cd字段
					String fullTableName = (String) tableInfo.get("as_table");
					if(fullTableName != null && fullTableName.trim().length() > 0) {
						List constCol = new ArrayList(); 
						List cols = (List) this.colMap.get(fullTableName);
						if(cols != null) {
							for(int z=0; z<cols.size(); z++) {
								Map col = (Map) cols.get(z);
								String colName = (String) col.get("COLUMNNAME");
								String is_const_cd = (String) col.get("IS_CONST_CD");
								if(is_const_cd.equalsIgnoreCase("Y")) constCol.add(colName);
							}
						}
						tableInfo.put("const_column_list", constCol);
					}
					
					Boolean ifVolitale = (Boolean) tableInfo.get("volatile");
					if (ifVolitale.booleanValue())
						volatileTableList.add(tableInfo);		//FIXME: 查看哪里使用
					else
						commonTableList.add(tableInfo);			//FIXME: 查看哪里使用
					parseTableWithSelect(statement);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.UpdateTable")) {
					parseUpdate(statement);
				}
				if (statement.getClassName().equals("com.teradata.sqlparser.interpret.AlterTable")) {
					parseAlterTable(statement);
				}
			} catch (Throwable e) {
				logger.error("=======><" + fileName + ">Sql sentence:\n" + sql + "\n", e);
				logger.error("=====================================================>");
				throw new MDSException(e);
			} finally {
				// System.gc();
			}
			if (sourceColumns != null)
				sourceColumns.clear();
			parser = null;
			statement = null;
		}
		return this.insertArray;
	}
}
