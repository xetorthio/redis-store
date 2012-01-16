package org.apache.catalina.session;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.Container;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.util.CustomObjectInputStream;

import org.apache.commons.pool.impl.GenericObjectPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisStore extends StoreBase implements Store {
    private static final int DEFAULT_DATABASE = 0;
    private static final byte[] DATA_FIELD = "data".getBytes();
    private static final byte[] ID_FIELD = "id".getBytes();
    private static Logger log = Logger.getLogger("RedisStore");
    
    private JedisPool jedisPool;
    /**
     * Redis Host
     */
    protected static String host = "localhost";

    /**
     * Redis Port
     */
    protected static int port = 6379;

    /**
     * Redis Password
     */
    protected static String password;

    /**
     * Redis database
     */
    protected static int database = DEFAULT_DATABASE;

    protected static boolean usePool = false;
    //Jedis Pool Settings
    protected static int maxIdle = GenericObjectPool.DEFAULT_MAX_IDLE;
    protected static int minIdle = GenericObjectPool.DEFAULT_MIN_IDLE;
    protected static int maxActive = GenericObjectPool.DEFAULT_MAX_ACTIVE;
    protected static long maxWait = GenericObjectPool.DEFAULT_MAX_WAIT;
    protected static byte whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    protected static boolean testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;
    protected static boolean testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;
    protected static boolean testWhileIdle = GenericObjectPool.DEFAULT_TEST_WHILE_IDLE;
    protected static long timeBetweenEvictionRunsMillis = GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    protected static int numTestsPerEvictionRun = GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
    protected static long minEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    protected static long softMinEvictableIdleTimeMillis = GenericObjectPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;

    public static boolean isUsePool() {
        return usePool;
    }

    public static void setUsePool(boolean usePool) {
        RedisStore.usePool = usePool;
    }

    public static int getMaxIdle() {
        return maxIdle;
    }

    public static void setMaxIdle(int maxIdle) {
        RedisStore.maxIdle = maxIdle;
    }

    public static int getMinIdle() {
        return minIdle;
    }

    public static void setMinIdle(int minIdle) {
        RedisStore.minIdle = minIdle;
    }

    public static int getMaxActive() {
        return maxActive;
    }

    public static void setMaxActive(int maxActive) {
        RedisStore.maxActive = maxActive;
    }

    public static long getMaxWait() {
        return maxWait;
    }

    public static void setMaxWait(long maxWait) {
        RedisStore.maxWait = maxWait;
    }

    public static byte getWhenExhaustedAction() {
        return whenExhaustedAction;
    }

    public static void setWhenExhaustedAction(byte whenExhaustedAction) {
        RedisStore.whenExhaustedAction = whenExhaustedAction;
    }

    public static boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public static void setTestOnBorrow(boolean testOnBorrow) {
        RedisStore.testOnBorrow = testOnBorrow;
    }

    public static boolean isTestOnReturn() {
        return testOnReturn;
    }

    public static void setTestOnReturn(boolean testOnReturn) {
        RedisStore.testOnReturn = testOnReturn;
    }

    public static boolean isTestWhileIdle() {
        return testWhileIdle;
    }

    public static void setTestWhileIdle(boolean testWhileIdle) {
        RedisStore.testWhileIdle = testWhileIdle;
    }

    public static long getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    public static void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        RedisStore.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public static int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public static void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        RedisStore.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public static long getMinEvictableIdleTimeMillis() {
        return minEvictableIdleTimeMillis;
    }

    public static void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
        RedisStore.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    public static long getSoftMinEvictableIdleTimeMillis() {
        return softMinEvictableIdleTimeMillis;
    }

    public static void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
        RedisStore.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
    }

    /**
     * Get the redis host
     * 
     * @return host
     */
    public static String getHost() {
        return host;
    }

    /**
     * Set redis host
     * 
     * @param host
     *            Redis host
     */
    public static void setHost(String host) {
        RedisStore.host = host;
    }

    /**
     * Get redis port. Defaults to 6379.
     * 
     * @return port
     */
    public static int getPort() {
        return port;
    }

    /**
     * Set redis port
     * 
     * @param port
     *            Redis port
     */
    public static void setPort(int port) {
        RedisStore.port = port;
    }

    /**
     * Get redis password
     * 
     * @return password
     */
    public static String getPassword() {
        return password;
    }

    /**
     * Set redis password
     * 
     * @param password
     *            Redis password
     */
    public static void setPassword(String password) {
        RedisStore.password = password;
    }

    /**
     * Get redis database
     * 
     * @return Redis database
     */
    public static int getDatabase() {
        return database;
    }

    /**
     * Set redis database
     * 
     * @param database
     *            Redis database. Defaults to 0.
     */
    public static void setDatabase(int database) {
        RedisStore.database = database;
    }

    private Jedis getJedis() throws IOException {
        Jedis jedis = null;
        if(usePool) {
            if (log.isLoggable(Level.INFO)) {
                log.info("Using pool");
            }
            if(jedisPool == null) {
                GenericObjectPool.Config config =  new GenericObjectPool.Config();
                config.maxActive = RedisStore.maxActive;
                config.maxIdle = RedisStore.maxIdle;
                config.maxWait = RedisStore.maxWait;
                config.minEvictableIdleTimeMillis = RedisStore.minEvictableIdleTimeMillis;
                config.minIdle = RedisStore.minIdle;
                config.numTestsPerEvictionRun = RedisStore.numTestsPerEvictionRun;
                config.softMinEvictableIdleTimeMillis = RedisStore.softMinEvictableIdleTimeMillis;
                config.testOnBorrow = RedisStore.testOnBorrow;
                config.testOnReturn = RedisStore.testOnReturn;
                config.testWhileIdle = RedisStore.testWhileIdle;
                config.timeBetweenEvictionRunsMillis = RedisStore.timeBetweenEvictionRunsMillis;
                
                if(RedisStore.password != null) {
                    jedisPool = new JedisPool(config, RedisStore.host, RedisStore.port, -1, RedisStore.password);
                } else {
                    jedisPool = new JedisPool(config, RedisStore.host, RedisStore.port);
                }
            }
            jedis = jedisPool.getResource();

        } else {
            if (log.isLoggable(Level.INFO)) {
                log.info("Using plain connection");
            }
            jedis = new Jedis(getHost(), getPort());
            jedis.connect();
        }
        if(RedisStore.getDatabase() != DEFAULT_DATABASE) {
            jedis.select(getDatabase());
        }

        
        return jedis;
    }

    private void closeJedis(Jedis jedis) throws IOException {
        if(usePool) {
            jedisPool.returnResource(jedis);
        } else {
            jedis.quit();
            jedis.disconnect();
        }

    }

    public void clear() throws IOException {
        Jedis jedis = getJedis();
        jedis.flushDB();
        closeJedis(jedis);
    }

    public int getSize() throws IOException {
        int size = 0;
        Jedis jedis = getJedis();
        size = jedis.dbSize().intValue();
        closeJedis(jedis);
        return size;
    }

    public String[] keys() throws IOException {
        String keys[] = null;
        Jedis jedis = getJedis();
        Set<String> keySet = jedis.keys("*");
        closeJedis(jedis);
        keys = keySet.toArray(new String[keySet.size()]);
        return keys;
    }

    public Session load(String id) throws ClassNotFoundException, IOException {
        StandardSession session = null;
        ObjectInputStream ois;
        Container container = manager.getContainer();
        Jedis jedis = getJedis();
        Map<byte[], byte[]> hash = jedis.hgetAll(id.getBytes());
        closeJedis(jedis);
        if (!hash.isEmpty()) {
            try {
                BufferedInputStream bis = new BufferedInputStream(
                        new ByteArrayInputStream(hash.get(DATA_FIELD)));
                Loader loader = null;
                if (container != null) {
                    loader = container.getLoader();
                }
                ClassLoader classLoader = null;
                if (loader != null) {
                    classLoader = loader.getClassLoader();
                }
                if (classLoader != null) {
                    ois = new CustomObjectInputStream(bis, classLoader);
                } else {
                    ois = new ObjectInputStream(bis);
                }
                session = (StandardSession) manager.createEmptySession();
                session.readObjectData(ois);
                session.setManager(manager);
                if (log.isLoggable(Level.INFO)) {
                    log.info("Loaded session id " + id);
                }
            } catch (Exception ex) {
                log.severe(ex.getMessage());
            }
        } else {
            log.warning("No persisted data object found");
        }
        return session;
    }

    public void remove(String id) throws IOException {
        Jedis jedis = getJedis();
        jedis.del(id);
        closeJedis(jedis);
        if (log.isLoggable(Level.INFO)) {
            log.info("Removed session id " + id);
        }
    }

    public void save(Session session) throws IOException {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream bos = null;

        Map<byte[], byte[]> hash = new HashMap<byte[], byte[]>();
        bos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(new BufferedOutputStream(bos));

        ((StandardSession) session).writeObjectData(oos);
        oos.close();
        oos = null;
        hash.put(ID_FIELD, session.getIdInternal().getBytes());
        hash.put(DATA_FIELD, bos.toByteArray());
        Jedis jedis = getJedis();
        jedis.hmset(session.getIdInternal().getBytes(), hash);
        closeJedis(jedis);
        if (log.isLoggable(Level.INFO)) {
            log.info("Saved session with id " + session.getIdInternal());
        }
    }
}
