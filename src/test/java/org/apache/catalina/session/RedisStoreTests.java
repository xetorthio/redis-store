package org.apache.catalina.session;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.catalina.Container;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.juli.logging.LogFactory;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisException;
import redis.clients.jedis.Protocol;

public class RedisStoreTests extends Assert {
    private Mockery context = new Mockery();
    private Manager manager = new PersistentManager();
    private RedisStore store;

    @Before
    public void setUp() {
	store = getStore();
    }

    @Test
    public void load() throws ClassNotFoundException, IOException,
	    JedisException {
	Session session = store.load("123");
	assertNull(session);

	Jedis jedis = new Jedis("localhost");
	jedis.connect();
	jedis.flushAll();
	Map<String, String> hash = new HashMap<String, String>();
	hash.put("data", "bar");
	jedis.hmset("//sampleApp123", hash);
	jedis.disconnect();

	session = store.load("123");
	assertNotNull(session);
    }

    private RedisStore getStore() {
	RedisStore.setHost("localhost");
	RedisStore.setPort(Protocol.DEFAULT_PORT);
	RedisStore.setDatabase(0);
	RedisStore rs = new RedisStore();

	final Container container = context.mock(Container.class);

	context.checking(new Expectations() {
	    {
		allowing(container).getName();
		will(returnValue("sampleApp"));
		allowing(container).getParent();
		will(returnValue(null));
		allowing(container).getLogger();
		will(returnValue(LogFactory.getLog("")));
	    }
	});

	manager = new PersistentManager();
	manager.setContainer(container);

	rs.setManager(manager);
	return rs;
    }
}
