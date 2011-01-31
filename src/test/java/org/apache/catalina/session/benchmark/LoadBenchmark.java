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

import redis.clients.jedis.Protocol;

public class LoadBenchmark {
    private static final int TOTAL_OPERATIONS = 1000;

    /**
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException,
            ClassNotFoundException {
        Logger.getLogger("RedisStore").setLevel(Level.OFF);
        RedisStore.setDatabase(0);
        RedisStore.setHost("localhost");
        RedisStore.setPassword("foobared");
        RedisStore.setPort(Protocol.DEFAULT_PORT);

        PersistentManager manager = new PersistentManager();
        manager.setContainer(new StandardContext());
        RedisStore rs = new RedisStore();
        rs.setManager(manager);

        Session session = manager.createSession(null);
        ((StandardSession) session).setAttribute("info", new String(
                new byte[30000]));
        rs.save(session);

        String id = session.getId();

        long begin = Calendar.getInstance().getTimeInMillis();
        for (int n = 0; n < TOTAL_OPERATIONS; n++) {
            rs.load(id);
        }
        long ellapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println((1000 * TOTAL_OPERATIONS / ellapsed)
                + " loads / second");
    }

}
