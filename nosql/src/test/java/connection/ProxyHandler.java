package connection;


import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * 动态代理handler
 *
 * @Author: Miaoxf
 * @Date: 2021/5/26 11:04
 */
public class ProxyHandler implements InvocationHandler {
    private static final String CLOSEMETHOD = "close";

    private RealConnection realConnection;

    private ConnectionPoolPractice connectionPool;

    public ProxyHandler(ConnectionPoolPractice connectionPool) {
        this.realConnection = new RealConnection();
        this.connectionPool = connectionPool;
    }

    public AbstractConnection createConnection() {
        return (AbstractConnection) Proxy.newProxyInstance(ProxyHandler.class.getClassLoader(),
                new Class[]{AbstractConnection.class}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals(CLOSEMETHOD)) {
            AbstractConnection conn = (AbstractConnection) proxy;
            return connectionPool.addConnection(conn);
        } else {
            return method.invoke(realConnection, args);
        }
    }
}
