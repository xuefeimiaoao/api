package tianz.bd.api.nosql.rocksdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;

/**
 * @Author: Miaoxf
 * @Date: 2021/1/28 20:02
 * @Description:
 */
public interface AbstractRocksDBConnection {

    public static final String defaultPath = getDefaultPath();
    public Long connectionTimeout = 10000L;
    public Integer maxIdledConn = 10;
    public Integer maxActiveConn = 10;

    public static String getDefaultPath() {
        String canonicalPath = "";
        try {
            canonicalPath = new File("").getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return canonicalPath + "\\metadata";
    }

    public String read(String inputKey) throws RocksDBException;

    public void put(ColumnFamilyHandle cfHandle, String inputKey, String inputValue) throws RocksDBException;

    public void put(String inputKey, String inputValue) throws RocksDBException;

    public void close();
}
