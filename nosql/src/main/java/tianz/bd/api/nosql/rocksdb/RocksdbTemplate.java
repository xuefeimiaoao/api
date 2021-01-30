package tianz.bd.api.nosql.rocksdb;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Miaoxf
 * @Date: 2021/1/19 9:53
 * @Description: Just for test
 */
public class RocksdbTemplate {

    static {
        RocksDB.loadLibrary();
    }

    private static RocksDB rdb;
    private static Options dbOpt;
    private static DBOptions dbOptions;
    private static final String defaultPath = getPath();

    public static void initPool() {

    }

    public static String getPath() {
        String canonicalPath = "";
        try {
            canonicalPath = new File("").getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return canonicalPath + "\\metadata";
    }

    public static void open() throws RocksDBException {
        // 配置当数据库不存在时自动创建
        dbOpt = new Options();
        dbOpt.setCreateIfMissing(true);
        String canonicalPath = null;
        rdb = RocksDB.open(dbOpt, defaultPath);
    }

    public static void open(String dbPath) {
        // 配置当数据库不存在时自动创建
        dbOpt = new Options();
        dbOpt.setCreateIfMissing(true);
        try {
            rdb = RocksDB.open(dbOpt, dbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public static void openReadOnly() throws RocksDBException {
        openReadOnly(defaultPath);
    }

    public static void openReadOnly(String dbPath) throws RocksDBException {
        dbOpt = new Options();
        dbOpt.setCreateIfMissing(true);
        rdb = RocksDB.openReadOnly(dbOpt, dbPath);
    }

    public static void openReadOnly(String dbPath, String cfName) {

    }

    public static List<ColumnFamilyHandle> openWithCF(String cfName) throws RocksDBException {
        return openWithCF(defaultPath, cfName);
    }

    public static List<ColumnFamilyHandle> openWithCF(String dbPath, String cfName) throws RocksDBException {
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        // list of column family descriptors, first entry must always be default column family
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts));

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        rdb = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
        return cfHandles;
    }

    public static void put(ColumnFamilyHandle cfHandle, String inputKey, String inputValue) throws RocksDBException {

        byte[] key = null;
        byte[] value = null;
        if (null != inputKey) {
            key = inputKey.getBytes();
        }
        if (null != inputValue) {
            value = inputValue.getBytes();
        }
        rdb.put(cfHandle, key, value);
    }

    public static void write(String inputKey, String inputValue) throws RocksDBException {
        //  写入数据
        //  RocksDB都是以字节流的方式写入数据库中，所以我们需要将字符串转换为字节流再写入。这点类似于HBase
        byte[] key = null;
        byte[] value = null;
        if (null != inputKey) {
            key = inputKey.getBytes();
        }
        if (null != inputValue) {
            value = inputValue.getBytes();
        }
        rdb.put(key, value);
    }


    public static String read(String inputKey) throws RocksDBException {
        byte[] key = inputKey.getBytes();

        String outputValue = null;
        try {
            byte[] values = rdb.get(key);
            if (null != values) {
                outputValue = rdb.get(key).toString();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return outputValue;
    }

    public static void close() {
        if (rdb != null && dbOpt != null) {
            rdb.close();
            dbOpt.close();
        }
    }
}
