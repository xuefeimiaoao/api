package tianz.bd.api.nosql.rocksdb;

import org.apache.commons.collections.CollectionUtils;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: Miaoxf
 * @Date: 2021/1/29 9:05
 * @Description:
 */
enum Mode {
    READ_ONLY,WRITE,ORDINAL
}

public class RocksDBConnectionPool {

    private List<RocksDBProxy> idledConnectionPool;
    private List<RocksDBProxy> activeConnectionPool;
    private RocksDBConnection activeWritableConnection;
    private Integer maxIdledConnection = 5;
    private Integer maxActiveConnection = 5;
    private Long maxWaitTime = 300000L;
    private Lock lock = new ReentrantLock();
    private Lock writeLock = new ReentrantLock();
    private Condition notEmpty = lock.newCondition();
    private Condition notFull = lock.newCondition();

    public RocksDBConnectionPool(Integer maxIdledConnection, Integer maxActiveConnection, Long maxWaitTime) {
        this();
        this.maxActiveConnection = maxActiveConnection;
        this.maxIdledConnection = maxIdledConnection;
        this.maxWaitTime = maxWaitTime;
    }

    public RocksDBConnectionPool() {
        this.idledConnectionPool = new ArrayList<>();
        this.activeConnectionPool = new ArrayList<>();
    }

    public AbstractRocksDBConnection getConnection() throws RocksDBException {
        return getConnection(null, Mode.READ_ONLY);
    }

    public AbstractRocksDBConnection getConnection(String dbPath) throws RocksDBException {
        return getConnection(dbPath, Mode.READ_ONLY);
    }

    public AbstractRocksDBConnection getConnection(Mode mode) throws RocksDBException {
        return getConnection(null, mode);
    }

    public AbstractRocksDBConnection getConnection(String dbPath, Mode mode) throws RocksDBException {

        //TODO 连接池根据dbPath分类，每个dbPath可以选择维护一个连接池；连接池中，只读模式可以创建多个连接，写模式只需要一个连接。
        switch (mode) {
            case WRITE:
                return getWritableConnection(dbPath);
            case READ_ONLY:
                return getReadOnlyConnection(dbPath);
            default:
                return getReadOnlyConnection(dbPath);
        }
    }

    private AbstractRocksDBConnection getWritableConnection(String dbPath) throws RocksDBException {
        if (dbPath == null) {
            return new RocksDBConnection(Mode.WRITE);
        } else {
            return new RocksDBConnection(dbPath, Mode.WRITE);
        }
    }

    private AbstractRocksDBConnection getReadOnlyConnection(String dbPath) throws RocksDBException {
        try {
            lock.lock();
            Boolean wait = false;

            //TODO idle超时

            //create connection when pool is empty
            while (CollectionUtils.isEmpty(idledConnectionPool)) {

                if (activeConnectionPool.size() < maxActiveConnection) {
                    RocksDBProxy rocksDBProxy;
                    if (null != dbPath) {
                        rocksDBProxy = new RocksDBProxy(new RocksDBConnection(dbPath), this);
                    } else {
                        rocksDBProxy = new RocksDBProxy(new RocksDBConnection(), this);
                    }
                    activeConnectionPool.add(rocksDBProxy);
                    return rocksDBProxy.getProxyDB();
                }

                //wait
                try {
                    notEmpty.await();
                } catch (InterruptedException e) {
                    wait = true;
                    break;
                }

            }

            if (wait) {
                return null;
            }

            RocksDBProxy rocksDBProxy = idledConnectionPool.get(0);
            idledConnectionPool.remove(0);
            return rocksDBProxy.getProxyDB();
        } finally {
            lock.unlock();
        }
    }

    public void addConnection(RocksDBProxy rocksDBProxy) {

        try {
            lock.lock();
            if (idledConnectionPool.size() < maxIdledConnection) {
                idledConnectionPool.add(rocksDBProxy);
                notEmpty.signal();
            } else {
                rocksDBProxy.getRealDB().close();
            }
        } finally {
            lock.unlock();
        }
    }
}

