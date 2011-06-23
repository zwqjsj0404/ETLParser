/**
 * Copyright 2007 By NCR China Co.Ltd. All rights reserved
 * 
 * Created on 2007-7-11
 */
package com.teradata.mds.etlparser.sqlparse;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.teradata.engine.mds.exception.MDSException;
import com.teradata.sqlparser.util.MDSSqlUtil;

/**
 * 从Teradata的Perl脚本日志中抽取SQL语句
 * 
 */
public class SQLExtractUtil {
	private Logger logger = Logger.getLogger(SQLExtractUtil.class);

	private String content = null;

	private static final String SELECT = "SELECT";

	private static final String INSERT = "INSERT";

	private static final String UPDATE = "UPDATE";

	private static final String CREATE = "CREATE";

	private static final String DELETE = "DELETE";

	private static final String DROP = "DROP";

	private static final String ALTER = "ALTER";

	private static final String REPLACE = "REPLACE";

	private static final String COMMA = ";";

	private static final String LINE = "\n";

	private static final String SPACE = " ";

	private static final String COMMENT_SIGN_BEGIN = "/*";

	private static final String COMMENT_SIGN_END = "*/";

	private static final String COMMENT_SIGN = "--";

	private static List TEMPLATE_TABLE_LIST = null;

	// 定义脚本日志中存在的非法字符串，进行替换
	private static final String[] illegalLexical = { "\u3000", "\u7236", "锘縍", "\u4e2a" };

	private static final String[] illegalLexicalReplace = { " ", " ", "R", " " };

	/**
	 * 
	 * @param content
	 *            带路径的文件名称，如d:/test/script/test.log
	 */
	public SQLExtractUtil(String content, List templateTableList) {
		this.content = content;
		this.TEMPLATE_TABLE_LIST = templateTableList;
	}

	/**
	 * 抽取SQL语句
	 * 
	 * @return
	 * @throws MDSException
	 */
	public List extractSqlList() throws MDSException {
		ArrayList retList = new ArrayList();
		FileReader fr = null;
		StringBuffer buf = null;
		try {
			logger.info("=======>" + content + "===================Begin");
			StringReader strReader = new StringReader(this.content);
			BufferedReader br = new BufferedReader(strReader);
			String line = br.readLine();
			while (line != null) {
				// 处理Tab符和注释行

				line = ignoreComment_Tab(line, br);
				if (line != null)
					line = replaceIllegalLexical(line);
				if (line == null)
					break;
				if (line.toUpperCase().startsWith(SELECT)// SQL语句开始

						|| line.toUpperCase().startsWith(INSERT)
						|| line.toUpperCase().startsWith(UPDATE)
						|| (line.toUpperCase().startsWith(CREATE) && (line.substring(7).trim().toUpperCase()
								.startsWith("VIEW") || line.substring(7).trim().toUpperCase().indexOf("TABLE") >= 0)) // 当不是create
																														// macro时

						|| line.toUpperCase().startsWith(DELETE)
						// ||line.toUpperCase().startsWith(DROP)
						|| line.toUpperCase().startsWith(ALTER)
						|| line.toUpperCase().startsWith(COMMA)
						|| (line.toUpperCase().startsWith(REPLACE) && line.substring(7).trim().toUpperCase()
								.startsWith("VIEW"))) // 当不是replace macro时

				{
					buf = new StringBuffer("");
					while (line != null)// 继续读入语句直到遇见分号表示当SQL语句结束
					{
						// 处理Tab符和注释行

						line = ignoreComment_Tab(line, br);
						line = replaceIllegalLexical(line);
						if (line != null && (line.startsWith(COMMA) || line.endsWith(COMMA)))// 解析分号位置，结束当前的SQL语句
						{
							int index = 0;
							if (line.startsWith(COMMA))
								index = 0;
							else if (line.endsWith(COMMA))
								index = line.length() - 1;
							String pre = line.substring(0, index);
							String suffer = line.substring(index + 1, line.length());
							if (pre.trim().length() == 0) {
								if (buf.length() > 0)
									buf.append(COMMA + LINE);
							} else if (pre.trim().length() > 0)
								buf.append(SPACE + pre + COMMA + LINE);

							if (suffer.trim().length() > 0) {
								line = suffer;
							} else
								line = br.readLine();

							if (buf.length() > 0) {
								if (!buf.toString().trim().toUpperCase().startsWith(DELETE)) {
									String tmpSQL = MDSSqlUtil.getTemplateTableForSQL(buf.toString(),
											this.TEMPLATE_TABLE_LIST);
									if (!retList.contains(tmpSQL)) {
										retList.add(tmpSQL);
										logger.info(tmpSQL);
									} else {
										// logger.error("[error:] "+tmpSQL);
									}

								}
							}
							break;
						} else {
							if (line.trim().length() > 0)
								buf.append(SPACE + line + LINE);
							line = br.readLine();
						}
					}
				} else
					line = br.readLine();
			}
			logger.info("=======>" + content + "=====================End\n\n");
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
			throw new MDSException(e);
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new MDSException(e);
		} finally {
			try {
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
		}
		replaceIllegalLexical(retList);
		return retList;
	}

	/**
	 * 去除Tab符以及对注释符的处理
	 * 
	 * @param line
	 * @param br
	 * @return
	 * @throws IOException
	 */
	private String ignoreComment_Tab(String line, BufferedReader br) throws IOException {
		line = replaceTab(line);
		if (line.trim().length() == 0) {
			return line;
		}

		if (line.startsWith(COMMENT_SIGN_BEGIN))// 避免读入注释符之间的SQL
		{
			while (line != null && !line.endsWith(COMMENT_SIGN_END)) {
				line = br.readLine();
				if (line != null)
					line = replaceTab(line);
			}
			if (line != null) {
				line = br.readLine();
				line = ignoreComment_Tab(line, br);
			}
		} else {
			while (line != null && line.startsWith(COMMENT_SIGN))// 以--开头的注释行

			{
				line = br.readLine();
				if (line != null)
					line = replaceTab(line);
			}
		}
		return line;
	}

	/**
	 * 替换Tab符
	 * 
	 * @param str
	 * @return
	 */
	private String replaceTab(String str) {
		Pattern p = Pattern.compile("\t");
		Matcher m = p.matcher(str);
		String after = m.replaceAll(" ");
		after = after.trim();
		return after;
	}

	/**
	 * 替换可能存在的非法字符
	 * 
	 * @param strList
	 */
	private void replaceIllegalLexical(List strList) {
		for (int i = 0; i < strList.size(); i++) {
			String line = (String) strList.get(i);
			for (int j = 0; j < illegalLexical.length; j++) {
				String illegal = illegalLexical[j];
				line = line.replaceAll(illegal, illegalLexicalReplace[j]);
			}
			strList.remove(i);
			strList.add(i, line);
		}
	}

	private String replaceIllegalLexical(String line) {
		for (int j = 0; j < illegalLexical.length; j++) {
			String illegal = illegalLexical[j];
			line = line.replaceAll(illegal, illegalLexicalReplace[j]);
		}
		return line;
	}
}
