package com.teradata.mds.etlparser;

import java.io.File;


/**
 * 纯文本文件类。<br>
 * 用此类管理处理过程中的纯文本文件。记录该文件的来源、、全路径文件名称及文件内容。
 * 此类适合对大量小文本文件做批量处理的程序。
 * @author md186003
 */
public class TxtFileHandler {
	private final String source;	//文件来源
	private final String path;		//相对路径
	private final String filename;	//文件名
	private final String perlFilename;	//Perl脚本原始名称
	private final String content;	//文件内容
	private final File file;		//文件句柄
	
	/**
	 * 初始化一个文件描述符
	 * @param source 文件来源(LOG, ETL等)
	 * @param path	文件相对路径
	 * @param filename 文件名称
	 * @param content 文件内容
	 * @param file 对应的File对象
	 */
	public TxtFileHandler(String source, String path, String filename, String content, File file) {
		this.source = source;
		this.path = path;
		this.filename = filename;
		this.content = content;
		this.file = file;
		if(filename != null) {
			int index = filename.indexOf(".pl");
			if(index == -1)
				this.perlFilename = filename;
			else
				this.perlFilename = filename.substring(0, index+3);
		} else {
			this.perlFilename = null;
		}
	}
	
	/*
	 * 取得文件来源系统
	 */
	public String getSource() {
		return source;
	}

	/*
	 * 取得文件名称
	 */
	public String getFilename() {
		return this.filename;
	}
	
	/*
	 * 取得Perl脚本的原始名称
	 */
	public String getPerlFilename() {
		return this.perlFilename;
	}

	/*
	 * 取得文件内容
	 */
	public String getContent() {
		return this.content;
	}

	/*
	 * 取得相对路径
	 */
	public String getPath() {
		return path;
	}

	/*
	 * 取得对应的File对象
	 */
	public File getFile() {
		return file;
	}
}
