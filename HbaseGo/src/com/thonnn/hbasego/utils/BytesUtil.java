package com.thonnn.hbasego.utils;

import java.io.*;

/**
 * 字节组转换工具
 * @author Thonnn 2017-11-26
 */
public class BytesUtil {
    /**
     * 对象转字节组，必须是可序列化的对象
     * @param obj 欲转换的对象
     * @return 转换结果字节组
     */
    public synchronized byte[] toBytes(Object obj){
        byte[] bytes = null;
        try {
            ByteArrayOutputStream baos = null;
            ObjectOutputStream oos;
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            bytes = baos.toByteArray();
            baos.close();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    /**
     * 字节组转对象，必须是可反序列化的
     * @param bytes 欲转换的字节组
     * @return 转换结果
     */
    public synchronized Object toObject(byte[] bytes){
        Object obj = null;
        try{
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            obj = ois.readObject();
            bais.close();
            ois.close();
        }catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        return obj;
    }
}
