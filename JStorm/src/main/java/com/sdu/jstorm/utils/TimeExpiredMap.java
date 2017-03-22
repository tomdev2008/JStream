package com.sdu.jstorm.utils;

import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Time;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hanhan.zhang
 * */
public class TimeExpiredMap<K, V> {

    /**
     * 默认{@link RotatingMap}分桶数量
     * */
    private static final int DEFAULT_NUM_BUCKETS = 3;

    /**
     * 读写锁,线程安全的保证
     * */
    private ReadWriteLock _readWriteLock = new ReentrantReadWriteLock(true);

    /**
     * 数据缓存Map
     * */
    private final RotatingMap<K, V> _rotatingMap;

    /**
     * 过期数据数据清空线程
     * */
    private final Thread _cleaner;

    /**
     * 过期数据
     * */
    private ExpiredCallback<K, V> _callback;

    public TimeExpiredMap(int expirationSecs, int numBuckets, final ExpiredCallback<K, V> callback) {
        _rotatingMap = new RotatingMap<K, V>(numBuckets);
        _callback = callback;

        /**
         * 如果在配置的时间内没有更新数据,这个数据就会被删
           expirationSecs * (1 + 1 / (numBuckets-1))解释:
           假设_cleaner线程刚刚清理数据,put函数调用发生将key放入桶中,那么一条数据的超时时间为：
           expirationSecs / (numBuckets-1) * numBuckets = expirationSecs * (1 + 1 / (numBuckets-1))
           然而假设put函数调用刚刚执行结束,_cleaner线程就开始清理数据,那么一条数据的超时时间为：
           expirationSecs / (numBuckets-1) * numBuckets - expirationSecs / (numBuckets-1) = expirationSecs
         * */
        final long expirationMillis = expirationSecs * 1000L;
        final long sleepTime = expirationMillis / (numBuckets-1);
        _cleaner = new Thread(() -> {
            while (true) {
                try {
                    Map<K, V> dead = null;
                    Time.sleep(sleepTime);
                    _readWriteLock.writeLock().lock();
                    dead = _rotatingMap.rotate();
                    if (_callback != null && dead != null) {
                        for (Map.Entry<K, V> entry : dead.entrySet()) {
                            _callback.expire(entry.getKey(), entry.getValue());
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } finally {
                    _readWriteLock.writeLock().unlock();
                }
            }
        });
        /**守护线程运行,一旦主线程结束,_cleaner线程自动结束*/
        _cleaner.setDaemon(true);
        _cleaner.start();
    }

    public TimeExpiredMap(int expirationSecs, ExpiredCallback<K, V> callback) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS, callback);
    }

    public TimeExpiredMap(int expirationSecs) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS);
    }

    public TimeExpiredMap(int expirationSecs, int numBuckets) {
        this(expirationSecs, numBuckets, null);
    }

    public boolean containsKey(K key) {
        boolean exist;
        _readWriteLock.readLock().lock();
        exist = _rotatingMap.containsKey(key);
        _readWriteLock.readLock().unlock();
        return exist;
    }

    public V get(K key) {
        V value = null;
        _readWriteLock.readLock().lock();
        value = _rotatingMap.get(key);
        _readWriteLock.readLock().unlock();
        return value;
    }

    public void put(K key, V value) {
        _readWriteLock.writeLock().lock();
        _rotatingMap.put(key, value);
        _readWriteLock.writeLock().unlock();
    }

    public Object remove(K key) {
        Object obj;
        _readWriteLock.writeLock().lock();
        obj =  _rotatingMap.remove(key);
        _readWriteLock.writeLock().unlock();
        return obj;
    }

    public int size() {
        int size;
        _readWriteLock.readLock().lock();
        size = _rotatingMap.size();
        _readWriteLock.readLock().unlock();
        return size;
    }

    /**
     * 禁止过期数据
     * */
    public void notExipireData() {
        /**
         * 中断清空线程
         * */
        _cleaner.interrupt();

    }

    /**
     * 数据清空回调接口
     * */
    public interface ExpiredCallback<K, V> {
        void expire(K key, V val);
    }

    public static void main(String[] args) throws InterruptedException {
        SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimeExpiredMap<String, Integer> timeExpiredMap = new TimeExpiredMap<String, Integer>(10, (key, value) -> {
            System.out.println("expire data [" + key + "," + value + "] at " + SDF.format(new Date()));
        });
        for (int i = 0; i < 10; ++i) {
            String key = UUID.randomUUID().toString();
            System.out.println("put data [" + key + "," + i + "] at " + SDF.format(new Date()));
            timeExpiredMap.put(key, i);
        }

        TimeUnit.MINUTES.sleep(3);
    }
}
