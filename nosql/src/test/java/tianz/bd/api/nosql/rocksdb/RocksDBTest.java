package tianz.bd.api.nosql.rocksdb;

import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: Miaoxf
 * @Date: 2021/1/28 14:17
 * @Description:
 */
public class RocksDBTest {

    SimpleDateFormat sf = new SimpleDateFormat();
    static RocksDBConnectionPool pool = new RocksDBConnectionPool();
    ExecutorService executorService = Executors.newFixedThreadPool(50);
    public static String path = null;
    public static String key = "testKey";
    public static String value = "hello";

    @Test
    public void test() {
        try {
            RocksdbTemplate.open("testPool");
            RocksdbTemplate.write("key1", "value1");
            System.out.println(RocksdbTemplate.read("key1"));
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            RocksdbTemplate.close();
        }
    }

    @Test
    public void testRead() {
        while (true) {
            ExecutorService service = Executors.newFixedThreadPool(10);
            for (int i = 1; i<=10; i++) {
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        singleRead();
                    }
                });
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getDefaultPath() {
        String canonicalPath = "";
        try {
            canonicalPath = new File("").getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return canonicalPath + "\\ttt";
    }

    @Test
    public void testWritePool() {
        try (AbstractRocksDBConnection connection = pool.getConnection(Mode.WRITE)) {
            connection.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKeepWritePool() {
        try (AbstractRocksDBConnection connection = pool.getConnection(Mode.WRITE)) {
            connection.put(key, value);
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReadPool() {
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        while (true) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    print(getDefaultPath(), key);
                }
            });
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void print(String path, String key) {
        try (AbstractRocksDBConnection connection = pool.getConnection(path)) {
            String read = connection.read(key);
            System.out.println(read);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReadOnly() {
        try {
            RocksdbTemplate.openReadOnly();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        while (true) {
            ExecutorService service = Executors.newFixedThreadPool(10);
            for (int i = 1; i<=10; i++) {
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        singleReadPool();
                    }
                });
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void singleRead() {
        try {
            RocksdbTemplate.open();
            System.out.println(getTime() + " " + RocksdbTemplate.read("key1"));
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            RocksdbTemplate.close();
        }
    }

    public void singleReadPool() {
        try {
            System.out.println(getTime() + " " + RocksdbTemplate.read("key1"));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }



    public String getTime() {
        long l = System.currentTimeMillis();
        Date date = new Date(l);
        return "currentTime:" + sf.format(date);
    }
}
