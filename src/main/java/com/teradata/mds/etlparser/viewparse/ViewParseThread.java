package com.teradata.mds.etlparser.viewparse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	public ViewParseThread(BlockingQueue viewQueue, BlockingQueue relationQueue, Map columns, Map tables, Map scripts) {
		this.viewQueue = viewQueue;
		this.relationQueue = relationQueue;
		this.columns = columns;
		this.tables = tables;
		this.scripts = scripts;
	}

	public void run() {
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
	}

	private List parseView(Map view) {
		List result = new ArrayList();

		try {
			StopWatch watch = new Log4JStopWatch("View Parser");
			List retList = new ArrayList();
			retList.add(view.get("VIEWSQL"));
			String db = (String) view.get("DB");
			String viewname = view.get("DB") + "." + view.get("VIEWNAME");
			
			//Added for CEB Debug
//			if(viewname.equalsIgnoreCase("dwmart_opn_cdrisk.CCD_CD_CARD_APPL_INFO")) {
//				System.out.println(viewname);
//			} else {
//				return result;
//			}
			
			SqlParser sqlparser = new SqlParser("", (HashMap) columns, (HashMap) tables, (HashMap)scripts);
			List result1 = sqlparser.bteqBridge(retList, (String) db);
			watch.lap("View Parser: Column Parse", viewname);
			tableRelationParser sqlparser2 = new tableRelationParser("", (HashMap) tables, (HashMap)this.scripts);
			List result2 = sqlparser2.bteqBridge(retList, db);
			watch.stop("View Parser: Table Parse", viewname);

			result.addAll(result1);
			result.addAll(result2);

			RelaParser.addSucess();
		} catch (MDSException e) {
			RelaParser.addFailed((String) view.get("DB")+"."+view.get("VIEWNAME"));
		}

		return result;

	}

}
