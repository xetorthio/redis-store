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
import java.util.List;
import java.util.Map;

import org.apache.catalina.Container;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.util.CustomObjectInputStream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisException;

public class RedisStore extends StoreBase implements Store {
    /**
     * Redis Host
     */
    protected static String host;

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

    private String name;

    public void clear() throws IOException {
	synchronized (this) {
	    Jedis jedis = getJedis();
	    try {
		jedis.flushDB();
	    } catch (JedisException e) {
		manager.getContainer().getLogger().error(
			"Cannot flush database", e);
	    }
	}
    }

    private Jedis getJedis() throws UnknownHostException, IOException {
	Jedis jedis = new Jedis(host);
	try {
	    jedis.setPort(getPort());
	    jedis.setHost(getHost());
	    jedis.connect();
	    jedis.select(getDatabase());
	} catch (JedisException e) {
	    manager.getContainer().getLogger().error(e.getMessage(), e);
	}
	return jedis;
    }

    public int getSize() throws IOException {
	int size = 0;

	synchronized (this) {
	    Jedis jedis = getJedis();
	    try {
		size = jedis.dbSize();
	    } catch (JedisException e) {
		manager.getContainer().getLogger().error(
			"Cannot get database size", e);
	    }
	}
	return size;
    }

    public String[] keys() throws IOException {
	String keys[] = null;
	synchronized (this) {
	    Jedis jedis = getJedis();
	    try {
		List<String> keysList = jedis.keys("*");
		keys = keysList.toArray(new String[keysList.size()]);
	    } catch (JedisException e) {
		manager.getContainer().getLogger().error(
			"Cannot get redis keys", e);
	    }
	}
	return keys;
    }

    public Session load(String id) throws ClassNotFoundException, IOException {
	StandardSession session = null;
	ObjectInputStream ois;
	synchronized (this) {
	    Jedis jedis = getJedis();
	    try {
		Container container = manager.getContainer();
		Map<String, String> hash = jedis.hgetAll(getName() + id);
		if (!hash.isEmpty()) {
		    BufferedInputStream bis = new BufferedInputStream(
			    new ByteArrayInputStream(hash.get("data")
				    .getBytes()));
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
		    //TODO: check if it is needed to set all the other data (ie. maxInterval, etc)
		    session.setManager(manager);
		} else {
		    if (manager.getContainer().getLogger().isDebugEnabled()) {
			manager.getContainer().getLogger().debug(
				"No persisted data object found");
		    }
		}
	    } catch (JedisException e) {
		manager.getContainer().getLogger().error(
			"Cannot get redis hash", e);
	    }
	}
	return session;
    }

    public void remove(String id) throws IOException {
	synchronized (this) {
	    try {
		getJedis().del(getName() + id);
	    } catch (JedisException e) {
		manager.getContainer().getLogger().error(
			"Cannot delete redis hash", e);
	    }
	}
    }

    public void save(Session session) throws IOException {
	ObjectOutputStream oos = null;
	ByteArrayOutputStream bos = null;

	synchronized (this) {
	    Map<String, String> hash = new HashMap<String, String>();
	    bos = new ByteArrayOutputStream();
	    oos = new ObjectOutputStream(new BufferedOutputStream(bos));

	    ((StandardSession) session).writeObjectData(oos);
	    oos.close();
	    oos = null;
	    hash.put("data", new String(bos.toByteArray()));
	    hash.put("id", session.getIdInternal());
	    hash.put("name", getName());
	    hash.put("valid", String.valueOf(session.isValid()));
	    hash.put("inactiveInterval", String.valueOf(session
		    .getMaxInactiveInterval()));
	    hash.put("lastAccessedTime", String.valueOf(session
		    .getLastAccessedTime()));
	    try {
		getJedis().hmset(getName() + session.getIdInternal(), hash);
	    } catch (JedisException e) {
		manager.getContainer().getLogger().error(
			"Cannot store redis hash", e);
	    }
	}
    }

    /**
     * Return the name for this instance (built from container name)
     */
    public String getName() {
	if (name == null) {
	    Container container = manager.getContainer();
	    String contextName = container.getName();
	    String hostName = "";
	    String engineName = "";

	    if (container.getParent() != null) {
		Container host = container.getParent();
		hostName = host.getName();
		if (host.getParent() != null) {
		    engineName = host.getParent().getName();
		}
	    }
	    name = "/" + engineName + "/" + hostName + contextName;
	}
	return name;
    }

}