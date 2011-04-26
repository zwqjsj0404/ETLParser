package com.teradata.mds.etlparser.relation;

public interface DataRelaVO {
	public static final int RELA_C = 0;
	public static final int RELA_T = 0;
	public static final char SEP_CHAR = 7;
	int getRelaType();
	String getInsertString();
	String getFastloadString();
	
	

}
