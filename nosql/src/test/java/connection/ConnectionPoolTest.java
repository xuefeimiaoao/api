package connection;

import org.junit.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * xxx
 *
 * @Author: Miaoxf
 * @Date: 2021/5/26 14:55
 */
public class ConnectionPoolTest {
    ConnectionPoolPractice connectionPool = new ConnectionPoolPractice();

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 10,
            0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    @Test
    public void test() throws InterruptedException {
        for (int i = 0; i < 500; i ++) {
            submit();
        }
        Thread.currentThread().join();
    }


    public void submit() {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try (AbstractConnection connection = connectionPool.getConnection()) {
                    connection.put("", "");
                    connection.read("");
                } catch (InterruptedException e) {
                }
            }
        });
    }
}
