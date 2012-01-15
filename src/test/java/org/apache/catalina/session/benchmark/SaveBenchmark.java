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

public class SaveBenchmark {
    private static final int TOTAL_OPERATIONS = 1001;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        SessionIdGenerator sessionIdGenerator = new SessionIdGenerator();
        Logger.getLogger("RedisStore").setLevel(Level.OFF);
        String info = new String(new byte[30000]);
        System.out.println(info.length());
        RedisStore.setHost("localhost");
        RedisStore.setPort(Protocol.DEFAULT_PORT);
        RedisStore.setUsePool(true);
        RedisStore.setMinIdle(40);
        RedisStore.setMaxActive(40);

        PersistentManager manager = new PersistentManager();
        manager.setContainer(new StandardContext());
        RedisStore rs = new RedisStore();
        rs.setManager(manager);

        long begin = Calendar.getInstance().getTimeInMillis();
        for (int n = 0; n < TOTAL_OPERATIONS; n++) {
            Session session = manager.createSession(sessionIdGenerator.generateSessionId());
            ((StandardSession) session).setAttribute("info", info);
            try {
                rs.save(session);
            } catch(RuntimeException re) {
                System.out.println("Broken in Operation: " + n);
                throw re;
            }
        }
        long ellapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println((1000 * TOTAL_OPERATIONS / ellapsed)
                + " saves / second");
    }

}