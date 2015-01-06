package com.autohome.adrd.algo.sessionlog.util;

/**
 * 
 * @author wang chao wangchao@autohome.com.cn
 * 
 */
public class LogSchema {

	public static final String[] ADLOG_SCHEMA = { "ADID", "COOKIE", "SessionId", "Bucketid" };

	public static final String[] SESSIONLOG_SCHEMA = { "user", "pv", "search", "adclick", "addisplay", "behavior", "saleleads", "tags", "extend", "filter", "userinfo", "apppv" ,"adclick_new","addisplay_new"};

	public static void main(String args[]) {

		for (String attr : ADLOG_SCHEMA) {
			System.out.format("%-17s %-10s\n", attr.toLowerCase(), "STRING,");
		}
	}

}
