package com.thonnn.hbasego.utils;

import com.thonnn.hbasego.exceptions.HbaseGoIDTailLengthException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * IDUtil 工具类用于获得一个ID，或者根据本类中定义的标准ID格式，将ID转化成时间戳
 * @author Thonnn
 * @version 1.1.1
 * @since 1.0.0
 */
public class IDUtil {
	private static int tailLength = 4;
	private static long rootNum = 10000;
	private static long currentNum = rootNum;
	private static SimpleDateFormat sdf = new SimpleDateFormat ("yyyyMMddHHmmssSSS");
	private static Random random = new Random();
	
	/**
	 * 从各种ID中获 Timestamp
	 * @param id 是一个String，长度必须大于17，会自动截取前17位，须服从Format ("yyyyMMddHHmmssSSS")
	 * @return Long Timestamp
	 * @throws ParseException String 转 Long Timestamp 错误时发生
	 * @since 1.0.0
	 */
	public static Long getTimestampFromID(String id) throws ParseException{
		String str = id.length() > 17 ? id.substring(0, 17) : id;
		Date date = sdf.parse(str);
		return date.getTime();
	}
	/**
	 * 默认情况下获取一个长 21 位的 ID 服从格式 yyyyMMddHHmmssSSS + 4 位尾数；
	 * 可以通过 setTailLength(int tailLength) 方法设定尾数的长度。
	 * @return 一个长度为 17  + 尾数长度 位数的 ID
	 * @since 1.0.0 使用随机数作为尾数，不支持自定义尾数长度。
	 * @since 1.1.1 使用循环自增数字作为尾数，支持自定义尾数长度。
	 */
	public static String getID(){
		return sdf.format(new Date()) + getTail();
	}

	/**
	 * 设置 ID 尾数长度。
	 * @param tailLength 尾数长度，取值在闭区间[4, 10]
	 * @since 1.1.1
	 */
	public static void setTailLength(int tailLength){
		if(tailLength <4 || tailLength > 10){
			try {
				throw new HbaseGoIDTailLengthException();
			} catch (HbaseGoIDTailLengthException e) {
				e.printStackTrace();
				return;
			}
		}
		IDUtil.tailLength = tailLength;
		currentNum = rootNum = (long) Math.pow(10, tailLength);
	}

	/**
	 * 获取尾数
	 * @return 尾数
	 * @since 1.1.1
	 */
	private static String getTail(){
		if (currentNum >= rootNum * 2){
			currentNum = rootNum;
		}
		String rsl = currentNum + "";
		currentNum++;
		return rsl.substring(1);
	}
}
