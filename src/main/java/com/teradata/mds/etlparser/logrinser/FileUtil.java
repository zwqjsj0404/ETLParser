package com.teradata.mds.etlparser.logrinser;

import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileUtil {
	private final String[] SQLTAGs;

	FileUtil(String[] sqltags) {
		SQLTAGs = sqltags;
	}

	/**
	 * 处理文件第一步:去掉垃圾行,去掉部分跨行的注释
	 * 
	 * @param srcPath
	 * @param desPath
	 */
	public StringBuffer processFile1(BufferedReader br) {
		StringBuffer sb = new StringBuffer();
		try {
			String line = "";
			// int loop = 0;
			while (line != null) {
				line = br.readLine();
				if (line == null)
					break;
				if (line.indexOf("DQID1") > -1 && line.indexOf("DQID1") < 3) /* 去掉DQ脚本不规范的地方 */
					line = line.replaceAll("DQID1", "; --DQID1");
				if (line.indexOf("TODay =") > -1 && line.indexOf("TODay =") < 3)
					line = line.replaceAll("TODay =", "; --TODay =");
				if (line.indexOf("CMIC--") > -1)
					line = line.replaceAll("CMIC--", "CMIC++");
				if (line.indexOf("NTFS--") > -1)
					line = line.replaceAll("NTFS--", "NTFS++");

				// 去掉 * FORMAT '-- 9')) 中的 -- '--
				line = line.replaceAll("FORMAT '\\x20*--", "FORMAT '");

				line = this.ignoreComment_Tab(line, br);
				if (line != null && !lineShouldBeRemoved(line))
					sb.append(line).append("\r");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		sb = removeComment(processBreakTag(sb));

		return sb;
	}

	/**
	 * 处理文件第二步:去掉空行,碰到特殊的SQL TAG进行特殊处理,普通行直接合并. 1 碰到整行就是一个SQL TAG 在前面后面加空格 2 碰到SQL TAG开头,前面加空格 3 碰到SQL TAG结尾,后面加空格 4
	 * 普通行不处理
	 * 
	 * @param srcPath
	 * @param desPath
	 */
	public String processFile2(StringBuffer sb) {
		String ret_value = null;
		StringReader reader = new StringReader(sb.toString());
		BufferedReader br = new BufferedReader(reader);
		StringBuffer newSB = new StringBuffer();
		String line = "";
		try {
			while (line != null) {
				line = br.readLine();
				newSB.append(processTag(br, line));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		ret_value = removeSpace(processResult(newSB.toString()));
		ret_value = ret_value.replaceAll("\r\\s*\r", "\r");
		ret_value = ret_value.replace((char) 0x0d, (char) 0x0a);

		return ret_value;
	}

	// /**
	// * 处理文件
	// * @param srcPath
	// * @param desPath
	// */
	// private void processFile(String srcPath,String desPath){
	// processFile1(srcPath, desPath+".tmp");
	// processFile2(desPath+".tmp",desPath);
	// new File(desPath+".tmp").delete();
	// }
	// /**
	// * 处理文件目录,仅限一层
	// * @param srcDir
	// * @param desDir
	// */
	// public static void processDir(String srcDir,String desDir){
	// File des = new File(desDir);
	// File src=new File(srcDir);
	// if (!des.exists()) {
	// des.mkdir();
	// }
	// FileUtil util=new FileUtil(SQLTAGs);
	// String[] fileList = src.list();
	// for (int i = 0; i < fileList.length; i++) {
	// if(util.chkFileName(fileList[i])){
	// util.processFile(srcDir + File.separator + fileList[i], desDir +
	// File.separator + fileList[i]);
	// }else{
	// continue;
	// }
	// }
	// }
	// /**
	// * 文件名是小写,且以log结束的文件为有效文件.
	// * @param fileName
	// * @return
	// */
	// private boolean chkFileName(String fileName){
	// Pattern p = Pattern.compile("[a-z0-9]{1,}(.*?)log");
	// Matcher m = p.matcher(fileName);
	// return m.matches();
	// }
	/**
	 * 
	 * @param br
	 * @param line
	 * @return
	 * @throws IOException
	 */
	private String processTag(BufferedReader br, String line) throws IOException {
		if (line == null || line.trim().equals("")) {
			return "";
		} else {
			if (lineEquals(line)) {
				return (" \r" + line + " \r");
			} else if (lineStartWith(line)) {
				if (lineEndWith(line)) {
					return (" \r" + line + " \r");
				} else {
					if (trailIsEmpty(line)) {
						return " \r" + line + " \r";
					} else {
						return " \r" + line;
					}
				}
			} else if (lineEndWith(line)) {
				if (headIsEmpty(line)) {
					return "\r " + line + " \r";
				} else {
					return line + " \r";
				}
			} else {
				if (headIsEmpty(line)) {
					if (trailIsEmpty(line)) {
						return "\r " + line + " \r";
					} else {
						return "\r " + line;
					}
				} else {
					if (trailIsEmpty(line)) {
						//FIXME: 修改为合并行，有可能引起其它地方错误
						return "\r" + line + "\r ";
						//return line + "\r";
					} else {
						//FIXME: 修改为合并行，有可能引起其它地方错误
						return "\r" + line + "\r";
						//return line;
					}
				}
			}
		}
	}

	/**
	 * 某行首字母是否是空格或者是\t
	 * 
	 * @param line
	 * @return
	 */
	private boolean headIsEmpty(String line) {
		Pattern p = Pattern.compile("^\\s+(.*)");
		Matcher m = p.matcher(line);
		if (m.matches())
			return true;
		return false;
	}

	/**
	 * 某行尾字母是否是空格或者是\t
	 * 
	 * @param line
	 * @return
	 */
	private boolean trailIsEmpty(String line) {
		Pattern p = Pattern.compile("(.*)\\s+$");
		Matcher m = p.matcher(line);
		if (m.matches())
			return true;
		return false;
	}

	/**
	 * 判断某行是否以某个SQL TAG为开始
	 * 
	 * @param line
	 * @return
	 */
	private boolean lineStartWith(String line) {
		// String[] tags=SQLTAGs.split("\\|");
		String[] tags = SQLTAGs;
		boolean result = false;
		for (int i = 0; i < tags.length; i++) {
			if (line.trim().toUpperCase().startsWith(tags[i])) {
				if (tags[i].equals(",") || tags[i].equals(";") || tags[i].equals("(")) {
					result = true;
				} else {
					String nextChar = line.trim().substring(tags[i].length(), tags[i].length() + 1);
					if (nextChar.equals(" ") || nextChar.equals("(")) {
						result = true;
					}
				}
			}
		}
		return result;
	}

	/**
	 * 判断某行是否以某个SQL TAG为结尾
	 * 
	 * @param line
	 * @return
	 */
	private boolean lineEndWith(String line) {
		// String[] tags=SQLTAGs.split("\\|");
		String[] tags = SQLTAGs;
		boolean result = false;
		for (int i = 0; i < tags.length; i++) {
			if (line.trim().toUpperCase().endsWith(tags[i])) {
				if (tags[i].equals(",") || tags[i].equals(";")) {
					result = true;
				} else {
					String beforeChar = line.trim().substring(line.trim().length() - tags[i].length() - 1,
							line.trim().length() - tags[i].length());
					if (beforeChar.equals(" ")) {
						result = true;
					}
				}
			}
		}
		return result;
	}

	/**
	 * 判断某行是否等于某个SQL TAG
	 * 
	 * @param line
	 * @return
	 */
	private boolean lineEquals(String line) {
		// String[] tags=SQLTAGs.split("\\|");
		String[] tags = SQLTAGs;
		boolean result = false;
		for (int i = 0; i < tags.length; i++) {
			if (line.trim().toUpperCase().equals(tags[i]))
				result = true;
		}
		return result;
	}

	/**
	 * 判断是否需要删掉整行,如果需要,返回TRUE,否则FALSE
	 * 
	 * @param line
	 * @return
	 */
	private boolean lineShouldBeRemoved(String line) {
		line = line.trim();
		// 空行去掉
		if (line.equals(""))
			return true;
		// 去掉$
		if (line.equals("$"))
			return true;
		// BT;ET;去掉
		if (line.toUpperCase().equals("BT;"))
			return true;
		if (line.toUpperCase().equals("ET;"))
			return true;
		// 去掉整行+---------
		Pattern p = Pattern.compile("^\\+-{2,}(.*)");
		Matcher m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 空格***空格[A-Z]
		p = Pattern.compile("^\\*{3} [A-Z](.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉Statement#行
		p = Pattern.compile("^Statement#(.*)");
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 .logon行
		p = Pattern.compile("^\\.LOGON(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 .WIDTH行
		p = Pattern.compile("^\\.WIDTH(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 .IF行
		p = Pattern.compile("^\\.IF (.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 .LABEL行
		p = Pattern.compile("^\\.LABEL(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 .LOGOFF行
		p = Pattern.compile("^\\.LOGOFF(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 .QUIT行
		p = Pattern.compile("^\\.QUIT(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 .GOTO行
		p = Pattern.compile("^\\.GOTO(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉 run_bteq_command行
		p = Pattern.compile("^run_bteq_command(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;
		// 去掉BTEQ行
		p = Pattern.compile("^BTEQ(.*)", Pattern.CASE_INSENSITIVE);
		m = p.matcher(line);
		if (m.matches())
			return true;

		return false;
	}

	/**
	 * 处理行
	 * 
	 * @param line
	 * @return
	 */
	private String processResult(String str) {
		Pattern p = Pattern.compile("--(.*?)\r");

		Matcher m = p.matcher(str);
		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			m.appendReplacement(sb, " \r");
		}
		m.appendTail(sb);
		return sb.toString();
	}

	/**
	 * 处理被打断的tag 比如/*和ORDER
	 */
	private StringBuffer processBreakTag(StringBuffer strBuffer) {
		StringBuffer sb = new StringBuffer();
		String str = strBuffer.toString();
		str = str.replaceAll("\\*\r/", "\\*/");
		str = str.replaceAll("/\r\\*", "/\\*");
		Pattern p = Pattern.compile("OR\rDER", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(str);
		while (m.find()) {
			m.appendReplacement(sb, "ORDER");
		}
		m.appendTail(sb);
		return sb;
	}

	/**
	 * 字符串中的多个空格合并成一个空格
	 * 
	 * @param s
	 * @return
	 */
	private String removeSpace(String s) {
		StringBuffer sb = new StringBuffer();
		Pattern p = Pattern.compile(" {1,}");
		Matcher m = p.matcher(s);
		while (m.find()) {
			m.appendReplacement(sb, " ");
		}
		m.appendTail(sb);
		return sb.toString();
	}

	/**
	 * 处理跨行的注释和title
	 */
	private StringBuffer removeComment(StringBuffer strBuffer) {
		StringBuffer sb = new StringBuffer();
		String s = strBuffer.toString();
		Pattern p = Pattern.compile("/\\*(.*?)\\*/", Pattern.DOTALL);
		Matcher m = p.matcher(s);
		while (m.find()) {
			m.appendReplacement(sb, " ");
		}
		m.appendTail(sb);

    //DELETE by MarkDong AT 2011-06-20
//		p = Pattern.compile("TITLE '(.*?)'", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
//		m = p.matcher(new String(sb));
//		sb = new StringBuffer();
//		while (m.find()) {
//			m.appendReplacement(sb, " ");
//		}
//		m.appendTail(sb);
		return sb;
	}

	/**
	 * 去除Tab符以及对独立注释符的处理
	 * 
	 * @param line
	 * @param br
	 * @return
	 * @throws IOException
	 */
	private String ignoreComment_Tab(String line, BufferedReader br) throws IOException {
		if (line == null)
			return line;

		line = replaceTab(line);

		if (line.trim().length() == 0) {
			return line;
		}

		if (line.indexOf("/*") > -1 && line.indexOf("*/") > -1)
			line = line.substring(0, line.indexOf("/*")) + line.substring(line.indexOf("*/") + 2, line.length());

		if (line.trim().startsWith("/*"))// 避免读入注释符之间的SQL
		{
			while (line != null && !line.trim().endsWith("*/")) {
				line = br.readLine();
				if (line != null)
					line = replaceTab(line);
			}
			if (line != null) {
				line = br.readLine();
				line = ignoreComment_Tab(line, br);
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
		// after=after.trim();
		return after;
	}
}
