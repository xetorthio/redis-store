package org.apache.catalina.session.test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.catalina.Session;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.PersistentManager;
import org.apache.catalina.session.RedisStore;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.SessionIdGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisStoreTest extends Assert {
    private PersistentManager manager;
    private RedisStore rs;
    private SessionIdGenerator sessionIdGenerator;

    @Before
    public void startUp() {
        RedisStore.setDatabase(0);
        RedisStore.setHost("localhost");
        //RedisStore.setPassword("foobared");
        RedisStore.setPort(Protocol.DEFAULT_PORT);

        manager = new PersistentManager();
        manager.setContainer(new StandardContext());
        rs = new RedisStore();
        rs.setManager(manager);
        sessionIdGenerator = new SessionIdGenerator();
    }

    @Test
    public void save() throws IOException, ClassNotFoundException {
        String sessionId = sessionIdGenerator.generateSessionId();
        System.out.println(sessionId);
        Session session = manager.createSession(sessionId);
        rs.save(session);

        Jedis j = new Jedis("localhost");
        j.connect();
        //j.auth("foobared");
        Map<String, String> data = j.hgetAll(sessionId);
        System.out.println(j.quit());
        j.disconnect();

        assertNotNull(data);
        ObjectOutputStream oos = null;
        ByteArrayOutputStream bos = null;

        bos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(new BufferedOutputStream(bos));

        ((StandardSession) session).writeObjectData(oos);
        oos.close();
        oos = null;
        assertEquals(session.getId(), data.get("id"));
    }

    @Test
    public void load() throws IOException, ClassNotFoundException {
        Session savedSession = manager.createSession(sessionIdGenerator.generateSessionId());
        ((StandardSession) savedSession).setAttribute("foo", "bar");
        rs.save(savedSession);

        Session loadedSession = rs.load(savedSession.getId());

        assertNotNull(loadedSession);
        assertEquals(savedSession.getId(), loadedSession.getId());
        assertEquals("bar", ((StandardSession) loadedSession)
                .getAttribute("foo"));
    }
}
