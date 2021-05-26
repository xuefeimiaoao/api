package connection;

import java.io.Closeable;

/**
 * xxx
 *
 * @Author: Miaoxf
 * @Date: 2021/5/26 10:38
 */
public interface AbstractConnection extends Closeable {
    String read(String key);

    int put(String key, String value);

    void close();
}
