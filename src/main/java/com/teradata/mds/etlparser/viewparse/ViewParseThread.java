package com.teradata.mds.etlparser.viewparse;

import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.teradata.db.DBQueryEngine;
import com.teradata.mds.etlparser.DBUtil;
import com.teradata.tap.system.query.QueryException;
import org.apache.log4j.Logger;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.teradata.engine.mds.exception.MDSException;
import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.relation.SQLParseResult;
import com.teradata.sqlparser.parser.SqlParser;
import com.teradata.sqlparser.parser.tableRelationParser;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

public class ViewParseThread implements Runnable {
  private static Logger logger = Logger.getLogger(ViewParseThread.class);

	private BlockingQueue viewQueue;
	private BlockingQueue relationQueue;
	private Map columns;
	private Map tables;
	private Map scripts;

  private DBQueryEngine engine = null;

	public ViewParseThread(BlockingQueue viewQueue, BlockingQueue relationQueue, Map columns, Map tables, Map scripts) {
		this.viewQueue = viewQueue;
		this.relationQueue = relationQueue;
		this.columns = columns;
		this.tables = tables;
		this.scripts = scripts;
	}

	public void run() {
    try {
      engine = DBUtil.getEngine();
    } catch (QueryException e) {
      engine = null;
    }

    while (true) {
			try {
				Object obj = viewQueue.take();
				if(obj instanceof String) {
					if(obj.equals(RelaParser.FINISH_STRING))	break;
				}
				
				Map view = (Map) obj;
				
				List relaData = parseView(view);

				if (relaData.size() > 0) {
					SQLParseResult result = new SQLParseResult((String) view.get("DB"), relaData);
					relationQueue.put(result);
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

    if(engine != null) {
      engine.removeDB();
    }
	}

	private List parseView(Map view) {
		List result = new ArrayList();

    List sqlList = new ArrayList();
    sqlList.add(view.get("VIEWSQL"));
    String db = (String) view.get("DB");
    String realdb = (String) view.get("REALDB");
    String viewname = view.get("DB") + "." + view.get("VIEWNAME");
    String realviewname = (String) view.get("VIEWNAME");

    if("DBC".equalsIgnoreCase(realdb)
            || "PDCRINFO".equalsIgnoreCase(realdb)
            || "EXPLAIN".equalsIgnoreCase(realdb)
            || "SQLJ".equalsIgnoreCase(realdb)
            || "SysAdmin".equalsIgnoreCase(realdb)
            || "Sys_Calendar".equalsIgnoreCase(realdb)
            || "dbcmngr".equalsIgnoreCase(realdb)
            || "dw_mds".equalsIgnoreCase(realdb)
       ) {
      logger.warn("Skip system views : " + realdb + "." + realviewname);
      RelaParser.addSucess();
      return result;
    }

		try {
			StopWatch watch = new Log4JStopWatch("View Parser");

			SqlParser sqlparser = new SqlParser("", (HashMap) columns, (HashMap) tables, (HashMap)scripts);
			List result1 = sqlparser.bteqBridge(sqlList, (String) db);
			watch.lap("View Parser: Column Parse", viewname);
			tableRelationParser sqlparser2 = new tableRelationParser("", (HashMap) tables, (HashMap)this.scripts);
			List result2 = sqlparser2.bteqBridge(sqlList, db);
			watch.stop("View Parser: Table Parse", viewname);

			result.addAll(result1);
			result.addAll(result2);
			RelaParser.addSucess();

      return result;
		} catch (MDSException e) {
      logger.warn("First view parse error, try load view from DBC.TableText");
		}

    //首次没解析成功，尝试从DBC加载View定义进行解析
    String getViewSql = "SELECT A1.DatabaseName"
                      + "     , A1.TableName"
                      + "     , A1.TableKind"
                      + "     , CASE WHEN A1.RequestTxtOverFlow IS NULL THEN A1.RequestText"
                      + "            ELSE A2.RequestText"
                      + "       END AS RequestText"
                      + "     , CASE WHEN A2.LineNo IS NULL THEN 1"
                      + "            ELSE A2.LineNo"
                      + "       END AS LineNo"
                      + "  FROM DBC.Tables A1"
                      + "  FULL JOIN DBC.TableText A2"
                      + "    ON A1.DatabaseName = A2.DatabaseName"
                      + "   AND A1.TableName = A2.TableName"
                      + "   AND A1.TableKind = A2.TableKind"
                      + " WHERE A1.DatabaseName = '" + realdb + "'"
                      + "   AND A1.TableName = '" + realviewname + "'"
                      + "   AND A1.TableKind = 'V'"
                      + " ORDER BY LineNo ASC";

    List views = null;
    try {
      views = getResultList(getViewSql);
    } catch (QueryException e1) {
      logger.error("Load view sql from DBC error!", e1);
      views = null;
    }

    if(views == null || views.size() == 0) {
      logger.error("Can't find view sql from DBC tabletext: " + realdb + "." + realviewname);
      RelaParser.addFailed(db+"."+realviewname);
      return result;
    }

    String viewSql = "";
    for(int i=0; i<views.size(); i++) {
      view = (Map) views.get(i);
      viewSql = viewSql + " " + view.get("REQUESTTEXT");
    }
    sqlList = new ArrayList();
    sqlList.add(viewSql);
    try {
      StopWatch watch = new Log4JStopWatch("View Parser");

      SqlParser sqlparser = new SqlParser("", (HashMap) columns, (HashMap) tables, (HashMap)scripts);
      List result1 = sqlparser.bteqBridge(sqlList, (String) db);
      watch.lap("View Parser: Column Parse", viewname);
      tableRelationParser sqlparser2 = new tableRelationParser("", (HashMap) tables, (HashMap)this.scripts);
      List result2 = sqlparser2.bteqBridge(sqlList, db);
      watch.stop("View Parser: Table Parse", viewname);

      result.addAll(result1);
      result.addAll(result2);
      RelaParser.addSucess();
    } catch (MDSException e) {
      RelaParser.addFailed(db+"."+realviewname);
    }

    return result;
	}

  private List getResultList(String query) throws QueryException {
    ArrayList list = new ArrayList();
    HashMap retMap = null;
    if (query == null || query.trim().equals(""))
      return list;
    try {
      engine.executeQuery(query);
      ResultSet rs = engine.getRs();
      ResultSetMetaData rsmd = rs.getMetaData();
      for (; rs.next(); list.add(retMap)) {
        retMap = new HashMap();
        for (int i = 0; i < rsmd.getColumnCount(); i++) {
          String colName = rsmd.getColumnName(i + 1);
          String value = rs.getString(i + 1);
          if (value == null)
            value = "";
          retMap.put(colName.toUpperCase(), value);
        }
      }
    } catch (QueryException qe) {
      throw qe;
    } catch (SQLException e) {
      QueryException qe = new QueryException(e.getMessage());
      qe.setErrorCode(String.valueOf(e.getErrorCode()));
      throw qe;
    }
    if (list.isEmpty())
      return null;
    else
      return list;
  }

}
