package com.teradata.mds.etlparser.relation;

/**
 * 表关联值对象
 */
public class DataRelaTableVO implements DataRelaVO{
	private String SrcObjId;
	private String TgtObjId;
	private String DataRelaModify;
	private int DataRelaCode;
	private int AutoTypeCode;
	private String DataRelaSrcId;
	private String DataRelaSrcName;
	
	private String Const_Cd_Col_Id;
	private String Const_Value;
	
	/**
	 * 创建表关联值对象
	 * @param srcObjId 源对象ID
	 * @param tgtObjId 目标对象ID
	 * @param dataRelaModify 关系修饰符
	 * @param dataRelaSrcId 关系来源对象
	 * @param dataRelaCode 数据关系代码
	 * @param autoTypeCode 关系添加方式
	 * @param dataRelaSrcName 关系来源对象名称
	 * @param constCdColId 常量字段ID 
	 * @param constValue 常量值
	 */
	public DataRelaTableVO(String srcObjId
						 , String tgtObjId
						 , String dataRelaModify
						 , String dataRelaSrcId
						 , int dataRelaCode
						 , int autoTypeCode
						 , String dataRelaSrcName
						 , String constCdColId
						 , String constValue) {
		super();
		SrcObjId = srcObjId;
		TgtObjId = tgtObjId;
		DataRelaModify = dataRelaModify;
		DataRelaSrcId = dataRelaSrcId;
		DataRelaCode = dataRelaCode;
		AutoTypeCode = autoTypeCode;
		DataRelaSrcName = dataRelaSrcName;
		
		Const_Cd_Col_Id = constCdColId;
		Const_Value = constValue;
	}
	
	/**
	 * 生成Insert语句
	 * @return insert 语句
	 */
	public String getInsertString() {
		StringBuffer sb = new StringBuffer("INSERT INTO ");
		sb.append("MT02_DATA_RELA_T");
		sb.append(" VALUES(");
		sb.append("'").append(this.SrcObjId).append("'");
		sb.append(", '").append(this.TgtObjId).append("'");
		sb.append(", '").append(this.DataRelaModify).append("'");
		sb.append(", ").append(this.DataRelaCode);
		sb.append(", ").append(this.AutoTypeCode);
		sb.append(", '").append(this.DataRelaSrcId).append("'");
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
		sb.append(RelationDataWriter.convertToFastload(this.DataRelaSrcName, 100));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.Const_Cd_Col_Id, 32));
		sb.append("  ");
		sb.append(RelationDataWriter.convertToFastload(this.Const_Value, 50));
		return sb.toString();
	}
	
	public int getRelaType() {
		return RELA_T;
	}
}