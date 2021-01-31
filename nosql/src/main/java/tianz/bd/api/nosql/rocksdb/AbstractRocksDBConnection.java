package tianz.bd.api.nosql.rocksdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @Author: Miaoxf
 * @Date: 2021/1/28 20:02
 * @Description:
 */
public interface AbstractRocksDBConnection extends Closeable {

    public Long connectionTimeout = 10000L;
    public Integer maxIdledConn = 10;
    public Integer maxActiveConn = 10;

    public String read(String inputKey) throws RocksDBException, UnsupportedEncodingException;

    public void put(ColumnFamilyHandle cfHandle, String inputKey, String inputValue) throws RocksDBException, UnsupportedEncodingException;

    public void put(String inputKey, String inputValue) throws RocksDBException, UnsupportedEncodingException;

    public void close() throws IOException;
}
