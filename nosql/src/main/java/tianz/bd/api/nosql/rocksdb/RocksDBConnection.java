package tianz.bd.api.nosql.rocksdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;


/**
 * @Author: Miaoxf
 * @Date: 2021/1/28 20:16
 * @Description:
 */
public class RocksDBConnection implements AbstractRocksDBConnection {

    static {
        RocksDB.loadLibrary();
    }

    public static final String defaultPath = getDefaultPath();
    private RocksDB realConn;
    private Options dbOpt;
    private String dbpath;
    private static final String format = "UTF-8";

    public static String getDefaultPath() {
        String canonicalPath = "";
        try {
            canonicalPath = new File("").getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return canonicalPath + "\\metadata";
    }

    //TODO 工厂模式
    public RocksDBConnection() throws RocksDBException {
        this.dbOpt = new Options();
        this.dbOpt.setCreateIfMissing(true);
        this.realConn = RocksDB.openReadOnly(this.dbOpt, defaultPath);
    }

    public RocksDBConnection(String dbpath) throws RocksDBException {
        this.dbOpt = new Options();
        this.dbOpt.setCreateIfMissing(true);
        this.realConn = RocksDB.openReadOnly(this.dbOpt, dbpath);
    }

    public RocksDBConnection(Mode mode) throws RocksDBException {
        this(defaultPath, mode);
    }

    public RocksDBConnection(String dbpath, Mode mode) throws RocksDBException {
        this.dbOpt = new Options();
        this.dbOpt.setCreateIfMissing(true);
        this.dbpath = dbpath;
        //TODO 根据Mode创建不同的db
        createRocksDB(dbpath, mode);
    }

    public void createRocksDB(String dbpath, Mode mode) throws RocksDBException {

        switch (mode) {
            case READ_ONLY:
                this.realConn = RocksDB.openReadOnly(this.dbOpt, dbpath);
            case WRITE:
                this.realConn = RocksDB.open(this.dbOpt, dbpath);
        }
    }

    @Override
    public String read(String inputKey) throws RocksDBException, UnsupportedEncodingException {

        byte[] key = inputKey.getBytes();
        String outputValue = null;
        byte[] values = realConn.get(key);
        if (null != values) {
            outputValue = new String(values, format);
        }
        return outputValue;
    }

    @Override
    public void put(ColumnFamilyHandle cfHandle, String inputKey, String inputValue) throws RocksDBException, UnsupportedEncodingException {

        byte[] key = null;
        byte[] value = null;
        if (null != inputKey) {
            key = inputKey.getBytes(format);
        }
        if (null != inputValue) {
            value = inputValue.getBytes(format);
        }
        realConn.put(cfHandle, key, value);
    }

    @Override
    public void put(String inputKey, String inputValue) throws RocksDBException, UnsupportedEncodingException {
        //  写入数据
        //  RocksDB都是以字节流的方式写入数据库中，所以我们需要将字符串转换为字节流再写入。这点类似于HBase
        byte[] key = null;
        byte[] value = null;
        if (null != inputKey) {
            key = inputKey.getBytes(format);
        }
        if (null != inputValue) {
            value = inputValue.getBytes(format);
        }
        realConn.put(key, value);
    }

    @Override
    public void close() throws IOException {
        if (this.realConn != null) {
            this.realConn.close();
        }
        if (this.dbOpt != null) {
            this.dbOpt.close();
        }
    }
}
