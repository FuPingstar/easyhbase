package com.xcar.hbase.common.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class EasyDateUtil {
	
	//通过Date拿到字符串
	public static String getString(Date date){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.000 z");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT")); 
		String timeString = sdf.format(date);
		return timeString;
	}

	public static String getString(Date date,String pattern){
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		String timeString = sdf.format(date);
		return timeString;
	}
	//通过String拿到日期
	public static Date getDate(String timeString){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.000 z");
		Date date = null;
		try {
			date = sdf.parse(timeString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}


}