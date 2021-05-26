package connection;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 练习 手写连接池
 *
 * @Author: Miaoxf
 * @Date: 2021/5/26 10:37
 */
public class ConnectionPoolPractice {
    private List<AbstractConnection> activeConns = new LinkedList<>();

    private List<AbstractConnection> idledConns = new LinkedList<>();

    /** 最大活跃的连接数 */
    private int maxActiveConnsNum = 5;

    /** 最大空闲的连接数 */
    private int maxIdledConnsNum = 5;

    private ReentrantLock lock = new ReentrantLock();

    /** 等待活跃的连接释放 */
    private Condition notFull = lock.newCondition();

    public AbstractConnection getConnection() throws InterruptedException {
        try {
            lock.lock();

            while (activeConns.size() <= maxActiveConnsNum) {
                if (activeConns.size() == maxActiveConnsNum) {
                    notFull.await();
                } else {
                    if (idledConns.size() == 0) {
                        ProxyHandler handler = new ProxyHandler(this);
                        AbstractConnection abstractConnection = handler.createConnection();
                        activeConns.add(abstractConnection);
                        return abstractConnection;
                    } else {
                        AbstractConnection connection = idledConns.get(0);
                        idledConns.remove(0);
                        activeConns.add(connection);
                        return connection;
                    }
                }
            }

            throw new RuntimeException("活跃连接数不能超过" + maxActiveConnsNum);
        } catch (InterruptedException e) {
            throw e;
        } finally {
            lock.unlock();
        }
    }

    public int addConnection(AbstractConnection conn) {
        try {
            lock.lock();
            if (idledConns.size() < maxIdledConnsNum) {
                idledConns.add(conn);
                //fixme 这边最好remove掉准确的obj
                activeConns.remove(0);
                notFull.signal();
                return 1;
            }
            return 0;
        } finally {
            lock.unlock();
        }
    }

}
