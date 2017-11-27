package com.thonnn.hbasego.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * IDUtil 工具类用于获得一个ID，或者根据本类中定义的标准ID格式，将ID转化成时间戳
 * @author Thonnn
 */
public class IDUtil {
	private static SimpleDateFormat sdf = new SimpleDateFormat ("yyyyMMddHHmmssSSS");
	private static Random random = new Random();
	
	/**
	 * 从各种ID中获 Timestamp
	 * @param id 是一个String，长度必须大于17，会自动截取前17位，须服从Format ("yyyyMMddHHmmssSSS")
	 * @return Long Timestamp
	 * @throws ParseException String 转 Long Timestamp 错误时发生
	 */
	public static Long getTimestampFromID(String id) throws ParseException{
		String str = id.length() > 17 ? id.substring(0, 17) : id;
		Date date = sdf.parse(str);
		return date.getTime();
	}

	/**
	 * 获取一个长 21 位的 ID 服从格式 yyyyMMddHHmmssSSS + 4 位随机数
	 * @return 一个长度为 21 位的 ID
	 */
	public static String getID(){
		return sdf.format(new Date()) + (((int)((random.nextFloat()+1)*10000)+"").substring(1));
	}
}
