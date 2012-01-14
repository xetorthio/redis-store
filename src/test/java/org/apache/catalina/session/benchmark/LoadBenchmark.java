package org.apache.catalina.session.benchmark;

import java.io.IOException;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.Session;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.PersistentManager;
import org.apache.catalina.session.RedisStore;
import org.apache.catalina.session.StandardSession;

import org.apache.catalina.util.SessionIdGenerator;
import redis.clients.jedis.Protocol;

public class LoadBenchmark {
    private static  int TOTAL_OPERATIONS = 100000;

    /**
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException,
            ClassNotFoundException {
        SessionIdGenerator sessionIdGenerator = new SessionIdGenerator();
        Logger.getLogger("RedisStore").setLevel(Level.OFF);
        RedisStore.setHost("localhost");
        //RedisStore.setPassword("foobared");
        RedisStore.setPort(Protocol.DEFAULT_PORT);
        RedisStore.setUsePool(true);
        RedisStore.setMinIdle(20);
        RedisStore.setMaxActive(20);

        PersistentManager manager = new PersistentManager();
        manager.setContainer(new StandardContext());
        RedisStore rs = new RedisStore();
        rs.setManager(manager);

        Session session = manager.createSession(sessionIdGenerator.generateSessionId());
        ((StandardSession) session).setAttribute("info", new String(
                new byte[30000]));
        rs.save(session);

        String id = session.getId();

        long begin = Calendar.getInstance().getTimeInMillis();
        for (int n = 0; n < TOTAL_OPERATIONS; n++) {
            try {
                rs.load(id);
            } catch(RuntimeException re) {
                System.out.println("Broken in Operation: " + n);
                throw re;
            }
        }
        long ellapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println((1000 * TOTAL_OPERATIONS / ellapsed)
                + " loads / second");
    }

}
