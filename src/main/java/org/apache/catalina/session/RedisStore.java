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

import redis.clients.jedis.Jedis;

public class RedisStore extends StoreBase implements Store {
    private static final byte[] DATA_FIELD = "data".getBytes();
    private static final byte[] ID_FIELD = "id".getBytes();
    private static Logger log = Logger.getLogger("RedisStore");
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
    protected static int database = 0;

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
        Jedis jedis = new Jedis(getHost(), getPort());
        jedis.connect();
        jedis.select(getDatabase());
        return jedis;
    }

    private void closeJedis(Jedis jedis) throws IOException {
        jedis.quit();
        jedis.disconnect();
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
