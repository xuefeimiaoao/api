package tianz.bd.api.nosql.rocksdb;


import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * @Author: Miaoxf
 * @Date: 2021/1/28 22:11
 * @Description:
 */
public class RocksDBProxy implements InvocationHandler,AbstractRocksDBConnection {

    //TODO 动态代理用的不对
    private RocksDBConnection realDB;
    private RocksDBConnectionPool pool;

    public RocksDBProxy(RocksDBConnection realDB, RocksDBConnectionPool pool) {
        this.realDB = realDB;
        this.pool = pool;
    }

    public RocksDBConnection getRealDB() {
        return realDB;
    }

    public void setRealDB(RocksDBConnection realDB) {
        this.realDB = realDB;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if(method.getName().equals("close")) {
            close();
            return null;
        } else {
            return method.invoke(realDB, args);
        }
    }

    @Override
    public String read(String inputKey) throws RocksDBException {
        return realDB.read(inputKey);
    }

    @Override
    public void put(ColumnFamilyHandle cfHandle, String inputKey, String inputValue) throws RocksDBException {
        realDB.put(cfHandle, inputKey, inputValue);
    }

    @Override
    public void put(String inputKey, String inputValue) throws RocksDBException {
        realDB.put(inputKey, inputValue);
    }

    @Override
    public void close() {
        this.pool.addConnection(this);
    }
}
