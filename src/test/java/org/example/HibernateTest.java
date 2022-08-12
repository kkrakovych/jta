package org.example;

import org.awaitility.Awaitility;
import org.example.model.Item;
import org.example.repository.ItemRepository;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SpringBootTest
class HibernateTest {

    private static final Logger logger = LoggerFactory.getLogger(HibernateTest.class);

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private ItemRepository itemRepository;

    @BeforeEach
    void setUp() {
        itemRepository.deleteAll();
    }

    @Test
    void testConfiguration() {
        Assertions.assertTrue(sessionFactory.getSessionFactoryOptions().isSecondLevelCacheEnabled());
        Assertions.assertTrue(sessionFactory.getSessionFactoryOptions().isQueryCacheEnabled());
    }

    private static Stream<Arguments> testMobileViewIssueOriginalParameters() {
        return Stream.of(
                Arguments.of(false),
                Arguments.of(true)
        );
    }

    @ParameterizedTest
    @MethodSource("testMobileViewIssueOriginalParameters")
    void testMobileViewIssueOriginal(boolean mainQueryCacheable) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                Session session = sessionFactory.openSession();
                Transaction tx = session.beginTransaction();

                Query<Item> q = session.createNamedQuery("Item.findByName");
                q.setParameter("name", "MobileView Issue");
                q.setCacheable(true);
                List<Item> actual = q.getResultList();
                logger.debug("Search {}", actual);
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

}
