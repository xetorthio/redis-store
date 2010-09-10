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
import java.util.logging.Logger;

import org.apache.catalina.Container;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.util.CustomObjectInputStream;

import redis.clients.jedis.Jedis;

public class RedisStore extends StoreBase implements Store {
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
	try {
	    jedis.connect();
	    jedis.select(getDatabase());
	    return jedis;
	} catch (UnknownHostException e) {
	    log.severe("Unknown redis host");
	    throw e;
	} catch (IOException e) {
	    log.severe("Unknown redis port");
	    throw e;
	}
    }

    private void closeJedis(Jedis jedis) throws IOException {
	jedis.quit();
	try {
	    jedis.disconnect();
	} catch (IOException e) {
	    log.severe(e.getMessage());
	    throw e;
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
	size = jedis.dbSize();
	closeJedis(jedis);
	return size;
    }

    public String[] keys() throws IOException {
	String keys[] = null;
	Jedis jedis = getJedis();
	List<String> keysList = jedis.keys("*");
	closeJedis(jedis);
	keys = keysList.toArray(new String[keysList.size()]);
	return keys;
    }

    public Session load(String id) throws ClassNotFoundException, IOException {
	StandardSession session = null;
	ObjectInputStream ois;
	Container container = manager.getContainer();
	Jedis jedis = getJedis();
	Map<String, String> hash = jedis.hgetAll(id);
	closeJedis(jedis);
	if (!hash.isEmpty()) {
	    try {
		BufferedInputStream bis = new BufferedInputStream(
			new ByteArrayInputStream(deserializeBytes(hash
				.get("data"))));
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
    }

    public void save(Session session) throws IOException {
	ObjectOutputStream oos = null;
	ByteArrayOutputStream bos = null;

	Map<String, String> hash = new HashMap<String, String>();
	bos = new ByteArrayOutputStream();
	oos = new ObjectOutputStream(new BufferedOutputStream(bos));

	((StandardSession) session).writeObjectData(oos);
	oos.close();
	oos = null;
	hash.put("id", session.getIdInternal());
	hash.put("data", serializeBytes(bos.toByteArray()));
	Jedis jedis = getJedis();
	jedis.hmset(session.getIdInternal(), hash);
	closeJedis(jedis);
    }

    public static String serializeBytes(byte[] a) {
	if (a == null) {
	    return "null";
	}
	if (a.length == 0) {
	    return "";
	}

	String byteString = null;
	byteString = serializeHexBytes(a);

	return byteString;
    }

    public static byte[] deserializeBytes(String sbytes) {
	byte[] bytes = null;
	bytes = deserializeHexBytes(sbytes);
	return bytes;
    }

    private static String serializeHexBytes(byte[] a) {
	StringBuilder hexString = new StringBuilder(2 * a.length);
	for (byte b : a) {
	    hexString.append("0123456789ABCDEF".charAt((b & 0xF0) >> 4))
		    .append("0123456789ABCDEF".charAt((b & 0x0F)));
	}
	return hexString.toString();
    }

    private static byte[] deserializeHexBytes(String sbytes) {
	byte[] bytes = new byte[sbytes.length() / 2];
	for (int i = 0, j = 0; i < bytes.length;) {
	    char upper = Character.toLowerCase(sbytes.charAt(j++));
	    char lower = Character.toLowerCase(sbytes.charAt(j++));
	    bytes[i++] = (byte) ((Character.digit(upper, 16) << 4) | Character
		    .digit(lower, 16));
	}
	return bytes;
    }
}
