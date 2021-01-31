package tianz.bd.api.nosql.rocksdb;


import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @Author: Miaoxf
 * @Date: 2021/1/28 22:11
 * @Description:
 */
public class RocksDBProxy implements InvocationHandler {

    private AbstractRocksDBConnection realDB;
    private AbstractRocksDBConnection proxyDB;
    private RocksDBConnectionPool pool;

    public RocksDBProxy(RocksDBConnection realDB, RocksDBConnectionPool pool) {
        this.realDB = realDB;
        this.proxyDB = (AbstractRocksDBConnection) Proxy.newProxyInstance(RocksDBConnection.class.getClassLoader(), RocksDBConnection.class.getInterfaces(), this);
        this.pool = pool;
    }

    public AbstractRocksDBConnection getRealDB() {
        return realDB;
    }

    public AbstractRocksDBConnection getProxyDB() {
        return proxyDB;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if(method.getName().equals("close")) {
            this.pool.addConnection(this);
            return null;
        } else {
            return method.invoke(realDB, args);
        }
    }
}
