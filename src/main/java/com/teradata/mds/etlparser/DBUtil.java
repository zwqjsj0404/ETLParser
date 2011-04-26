package com.teradata.mds.etlparser;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.teradata.db.DBQueryEngine;
import com.teradata.tap.system.query.QueryException;

public class DBUtil {
	private static String jdbcDriver;
	private static String jdbcUrl;
	private static String jdbcUser;
	private static String jdbcPwd;
	
	static {
		try {
			InputStream is = new FileInputStream("tap.properties");
			Properties props = new Properties();
			props.load(is);
			jdbcDriver = props.getProperty("jdbcDriver");
			jdbcUrl = props.getProperty("jdbcUrl");
			jdbcUser = props.getProperty("jdbcUser");
			jdbcPwd = props.getProperty("jdbcPwd");
			
			is.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static DBQueryEngine getEngine() throws QueryException {
		DBQueryEngine engine = new DBQueryEngine();
    	engine.createConnection(jdbcDriver, jdbcUrl, jdbcUser, jdbcPwd);
    	engine.setAutoCommit(false);
    	return engine;
	}
}
