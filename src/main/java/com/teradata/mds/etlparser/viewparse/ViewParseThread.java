package com.teradata.mds.etlparser.viewparse;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.teradata.db.DBQueryEngine;
import com.teradata.mds.etlparser.DBUtil;
import com.teradata.tap.system.query.QueryException;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.teradata.engine.mds.exception.MDSException;
import com.teradata.mds.etlparser.RelaParser;
import com.teradata.mds.etlparser.relation.SQLParseResult;
import com.teradata.sqlparser.parser.SqlParser;
import com.teradata.sqlparser.parser.tableRelationParser;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

public class ViewParseThread implements Runnable {
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

		}

    //首次没解析成功，尝试从DBC加载View定义进行解析
    String getViewSql = "SELECT DatabaseName"
                      + "     , TableName"
                      + "     , TableKind"
                      + "     , RequestText"
                      + "     , LineNo"
                      + "  FROM DBC.TableText"
                      + " WHERE DatabaseName = '" + realdb + "'"
                      + "   AND TableName = '" + viewname + "'"
                      + "   AND TableKind = 'V'"
                      + " ORDER BY LineNo ASC";

    List views = null;
    try {
      views = engine.getResultList(getViewSql);
    } catch (QueryException e1) {
      views = null;
    }

    if(views == null || views.size() == 0) {
      RelaParser.addFailed((String) view.get("DB")+"."+view.get("VIEWNAME"));
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

      return result;
    } catch (MDSException e) {
      RelaParser.addFailed((String) view.get("DB")+"."+view.get("VIEWNAME"));
    }

    return result;
	}

}
