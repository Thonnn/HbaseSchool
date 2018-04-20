package com.thonnn.hbasego.ddl;

import com.thonnn.hbasego.utils.BytesUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.Serializable;

/**
 * 构造的列簇类，使用此类时请尤其注意，你必须十分熟悉 Hbase 表结构中各个属性对应的功能才可以进行大量的配置。否则可能会引发大量未知的错误。<br>
 * 一般来说，你只需要配置 NAME 属性和 MAX_VERSIONS 属性就足够了，其他的推荐使用默认。<br>
 * <a href="http://hbase.apache.org/1.2/apidocs/org/apache/hadoop/hbase/HColumnDescriptor.html">点此查阅Hbase 1.2.6 官方对应文档。</a>
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public final class HbaseGoTableFamily implements Serializable {
    /**
     * Hbase 基础属性：表名称，无默认值，必须由用户定义
     * @since 1.2.0
     */
    public String NAME;
    /**
     * Hbase 基础属性：布隆过滤器，推荐使用默认值
     * @since 1.2.0
     */
    public BloomType BLOOMFILTER = BloomType.ROW;
    /**
     * Hbase 基础属性：存储最大版本的数量，默认 1
     * @since 1.2.0
     */
    public int MAX_VERSIONS = 1;
    /**
     * Hbase 基础属性：常驻缓存，推荐使用默认值
     * @since 1.2.0
     */
    public boolean IN_MEMORY = false;
    /**
     * Hbase 基础属性：保留删除的数据，推荐使用默认值
     * @since 1.2.0
     */
    public KeepDeletedCells KEEP_DELETED_CELLS = KeepDeletedCells.FALSE;
    /**
     * Hbase 基础属性：数据块编码，推荐使用默认值
     * @since 1.2.0
     */
    public DataBlockEncoding DATA_BLOCK_ENCODING = DataBlockEncoding.NONE;
    /**
     * Hbase 基础属性：数据超时时间，推荐使用默认值
     * @since 1.2.0
     */
    public int TTL = HConstants.FOREVER;
    /**
     * Hbase 基础属性：数据压缩，推荐使用默认值
     * @since 1.2.0
     */
    public Compression.Algorithm COMPRESSION = Compression.Algorithm.NONE;
    /**
     * Hbase 基础属性：最小版本数，推荐使用默认值<br>
     * 事实上 Hbase 本身并不允许使用 0 或负数作为版本数，但是奇怪的是，如果你不尝试设定最小版本数，它有可能会有一个默认值 0
     * @since 1.2.0
     */
    public int MIN_VERSIONS = 0;
    /**
     * Hbase 基础属性：块高速缓存，推荐使用默认值
     * @since 1.2.0
     */
    public boolean BLOCKCACHE = true;
    /**
     * Hbase 基础属性：块大小，推荐使用默认值，单位 byte
     * @since 1.2.0
     */
    public int BLOCKSIZE = 65536;
    /**
     * Hbase 基础属性：复制范围，推荐使用默认值
     * @since 1.2.0
     */
    public int REPLICATION_SCOPE = 0;

    /**
     * 使用列簇名进行构造
     * @param familyName 列簇名
     * @since 1.2.0
     */
    public HbaseGoTableFamily(String familyName){
        NAME = familyName;
    }

    /**
     * 根据已有配置克隆一个配置相同的列簇，此克隆方式为深度克隆。
     * @param newName 克隆结果的列簇名
     * @return 克隆出的列簇对象
     * @since 1.2.0
     */
    public HbaseGoTableFamily cloneThis(String newName){
        BytesUtil bytesUtil = new BytesUtil();
        HbaseGoTableFamily family = (HbaseGoTableFamily) bytesUtil.toObject(bytesUtil.toBytes(this));
        family.NAME = newName;
        return family;
    }
}
