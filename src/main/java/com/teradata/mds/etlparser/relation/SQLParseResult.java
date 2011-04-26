package com.teradata.mds.etlparser.relation;

import java.util.List;

public class SQLParseResult {
	private String logfilename;
	private List relas;
	
	public SQLParseResult(String name, List relas) {
		this.logfilename = name;
		this.relas = relas;
	}
	
	public String getLogfileName() {
		return this.logfilename;
	}
	
	public List getRelas() {
		return this.relas;
	}
}
