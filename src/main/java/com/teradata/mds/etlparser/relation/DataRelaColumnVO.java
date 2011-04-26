package com.teradata.mds.etlparser.relation;

public class DataRelaColumnVO implements DataRelaVO {
	private String SrcObjId;
	private String TgtObjId;
	private String DataRelaModify;
	private int DataRelaCode;
	private int AutoTypeCode;
	private String DataRelaSrcId;
	private String RelaDesc;
	private String DataRelaSrcName;
	
	private String Const_Cd_Col_Id;
	private String Const_Value;
	
	public DataRelaColumnVO(String srcObjId, String tgtObjId, String dataRelaModify, int dataRelaCode,
			int autoTypeCode, String dataRelaSrcId, String relaDesc, String dataRelaSrcName
			, String constColId, String constValue) {
		super();
		SrcObjId = srcObjId;
		TgtObjId = tgtObjId;
		DataRelaModify = dataRelaModify;
		DataRelaCode = dataRelaCode;
		AutoTypeCode = autoTypeCode;
		DataRelaSrcId = dataRelaSrcId;
		RelaDesc = relaDesc;
		DataRelaSrcName = dataRelaSrcName;
		Const_Cd_Col_Id = constColId;
		Const_Value = constValue;
	}
	
	/**
	 * 生成Insert语句
	 * @return insert 语句
	 */
	public String getInsertString() {
		StringBuffer sb = new StringBuffer("INSERT INTO ");
		sb.append("MT02_DATA_RELA_C");
		sb.append(" VALUES(");
		sb.append("'").append(this.SrcObjId).append("'");
		sb.append(", '").append(this.TgtObjId).append("'");
		sb.append(", '").append(this.DataRelaModify).append("'");
		sb.append(", ").append(this.DataRelaCode);
		sb.append(", ").append(this.AutoTypeCode);
		sb.append(", '").append(this.DataRelaSrcId).append("'");
		sb.append(", '").append(this.RelaDesc).append("'");
		sb.append(", '").append(this.DataRelaSrcName).append("'");
		sb.append(", '").append(this.Const_Cd_Col_Id).append("'");
		sb.append(", '").append(this.Const_Value).append("'");
		sb.append(");");
		return sb.toString();
	}
	
	
	
	public String getFastloadString() {
		StringBuffer sb = new StringBuffer();
		sb.append(RelationDataWriter.convertToFastload(this.SrcObjId, 32));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.TgtObjId, 32));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.DataRelaModify, 20));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(String.valueOf(this.DataRelaCode), 2));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(String.valueOf(this.AutoTypeCode), 2));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.DataRelaSrcId, 32));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.RelaDesc, 3500));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.DataRelaSrcName, 100));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.Const_Cd_Col_Id, 32));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.Const_Value, 50));
		return sb.toString();
	}
	
	public int getRelaType() {
		return RELA_C;
	}
}
