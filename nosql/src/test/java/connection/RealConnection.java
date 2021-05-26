package connection;

/**
 * xxx
 *
 * @Author: Miaoxf
 * @Date: 2021/5/26 11:08
 */
public class RealConnection implements AbstractConnection {
    @Override
    public String read(String key) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        }
        System.out.println("read successfully!");
        return null;
    }

    @Override
    public int put(String key, String value) {
        System.out.println("put successfully!");
        return 0;
    }

    @Override
    public void close() {
        System.out.println("close!");
    }
}
