package org.example;

import org.awaitility.Awaitility;
import org.example.model.Item;
import org.example.repository.ItemRepository;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.jpa.QueryHints;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SpringBootTest
class HibernateTest {

    private static final Logger logger = LoggerFactory.getLogger(HibernateTest.class);

    @Autowired
    private ItemRepository itemRepository;

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @BeforeEach
    void setUp() {
        itemRepository.deleteAll();
    }

    @Test
    void testConfiguration() {
        Assertions.assertTrue(sessionFactory.getSessionFactoryOptions().isSecondLevelCacheEnabled());
        Assertions.assertTrue(sessionFactory.getSessionFactoryOptions().isQueryCacheEnabled());
    }

    private static Stream<Arguments> testMobileViewIssueParameters() {
        return Stream.of(
                Arguments.of(Boolean.FALSE),
                Arguments.of(Boolean.TRUE)
        );
    }

    private boolean isCacheable(Query<Item> q) {
        return q.isCacheable();
    }

    private boolean isCacheable(javax.persistence.Query q) {
        org.hibernate.Query<Item> hq = q.unwrap(org.hibernate.Query.class);
        return isCacheable(hq);
    }

    @ParameterizedTest
    @MethodSource("testMobileViewIssueParameters")
    void testMobileViewIssue_SessionFactory(boolean mainQueryCacheable) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                Session session = sessionFactory.openSession();
                session.setFlushMode(FlushModeType.COMMIT);
                Transaction tx = session.beginTransaction();

                Query<Item> q = session.createNamedQuery("Item.findByName");
                q.setParameter("name", "MobileView Issue");
                q.setCacheable(true);
                List<Item> actual = q.getResultList();
                logger.debug("Search {}, isCacheable={}", actual, isCacheable(q));
                Assertions.assertNotNull(actual);
                Assertions.assertEquals(0, actual.size());

                tx.commit();
                session.close();
            }
        }, 0, 1000);

        Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).untilAsserted(() -> Assertions.assertTrue(true));

        Session session = sessionFactory.openSession();
        Transaction tx = session.beginTransaction();

        logger.debug("Item: creating");
        Item expected = new Item("MobileView Issue");
        session.save(expected);
        session.flush();
        logger.debug("Item: created");

        Awaitility.await().pollDelay(2000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assertions.assertTrue(true));

        logger.debug("Item: search");
        Query<Item> q = session.createNamedQuery("Item.findByName");
        q.setParameter("name", "MobileView Issue");
        q.setCacheable(mainQueryCacheable);
        logger.debug("Item: searching isCacheable={}", isCacheable(q));
        List<Item> actual = q.getResultList();
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual.get(0));
        logger.debug("Item: found {}", actual.get(0));

        tx.commit();
        session.close();

        timer.cancel();
        timer.purge();
    }

    @ParameterizedTest
    @MethodSource("testMobileViewIssueParameters")
    void testMobileViewIssue_EntityManagerFactory(boolean mainQueryCacheable) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                EntityManager entityManager = entityManagerFactory.createEntityManager();
                EntityTransaction et = entityManager.getTransaction();
                et.begin();

                javax.persistence.Query q = entityManager.createNamedQuery("Item.findByName");
                q.setParameter("name", "MobileView Issue");
                q.setHint(QueryHints.HINT_CACHEABLE, Boolean.TRUE);
                List<Item> actual = (List<Item>) q.getResultList();
                logger.debug("Search {}, isCacheable={}", actual, isCacheable(q));
                Assertions.assertNotNull(actual);
                Assertions.assertEquals(0, actual.size());

                et.commit();
                entityManager.close();
            }
        }, 0, 1000);

        Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).untilAsserted(() -> Assertions.assertTrue(true));

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.setFlushMode(FlushModeType.COMMIT);
        EntityTransaction et = entityManager.getTransaction();
        et.begin();

        logger.debug("Item: creating");
        Item expected = new Item("MobileView Issue");
        entityManager.persist(expected);
        entityManager.flush();
        logger.debug("Item: created");

        Awaitility.await().pollDelay(2000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assertions.assertTrue(true));

        logger.debug("Item: search");
        javax.persistence.Query q = entityManager.createNamedQuery("Item.findByName");
        q.setParameter("name", "MobileView Issue");
        q.setHint(QueryHints.HINT_CACHEABLE, mainQueryCacheable);
        logger.debug("Item: searching isCacheable={}", isCacheable(q));
        List<Item> actual = (List<Item>) q.getResultList();
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual.get(0));
        logger.debug("Item: found {}", actual.get(0));

        et.commit();
        entityManager.close();

        timer.cancel();
        timer.purge();
    }

}
