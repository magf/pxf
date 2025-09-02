package org.greenplum.pxf.service;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.Fragmenter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.FragmenterCacheFactory;
import org.greenplum.pxf.service.fragment.*;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.greenplum.pxf.service.utilities.GSSFailureHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FragmenterServiceTest {
    @Mock
    private BasePluginFactory mockPluginFactory;
    @Mock
    private Fragmenter fragmenter1;
    @Mock
    private Fragmenter fragmenter2;
    @Mock
    private Fragmenter fragmenter3;
    @Mock
    private FragmenterCacheFactory fragmenterCacheFactory;
    @Mock
    private FragmentStrategyProvider strategyProvider;
    @Mock
    private FragmentStrategy strategy;

    private Cache<String, List<Fragment>> fragmentCache;
    private FakeTicker fakeTicker;
    private FragmenterService fragmenterService;
    private Configuration configuration;
    private RequestContext context1;
    private RequestContext context2;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        context1 = new RequestContext();
        context1.setTransactionId("XID-XYZ-123456");
        context1.setFragmenter("org.greenplum.pxf.api.model.Fragmenter1");
        context1.setSegmentId(0);
        context1.setGpCommandCount(1);
        context1.setGpSessionId(1);
        context1.setTotalSegments(1);
        context1.setDataSource("path.A");
        context1.setConfiguration(configuration);

        context2 = new RequestContext();
        context2.setTransactionId("XID-XYZ-654321");
        context2.setFragmenter("org.greenplum.pxf.api.model.Fragmenter2");
        context2.setSegmentId(0);
        context2.setGpCommandCount(1);
        context2.setGpSessionId(1);
        context2.setTotalSegments(1);
        context2.setDataSource("path.A");
        context2.setConfiguration(configuration);

        fakeTicker = new FakeTicker();
        fragmentCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .ticker(fakeTicker)
                .build();

        lenient().when(fragmenterCacheFactory.getCache()).thenReturn(fragmentCache);
        lenient().when(strategyProvider.getStrategy(any(RequestContext.class))).thenReturn(strategy);
        lenient().when(strategy.filterFragments(any(), any())).thenReturn(Collections.emptyList());

        // use a real handler to ensure pass-through calls on default configuration
        fragmenterService = new FragmenterService(fragmenterCacheFactory,
                mockPluginFactory, new GSSFailureHandler(), strategyProvider);
    }

    @Test
    public void getFragmentsResponseFromEmptyCache() throws Throwable {
        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(fragmenter1.getFragments()).thenReturn(Collections.emptyList());
        fragmenterService.getFragmentsForSegment(context1);
        verify(fragmenter1, times(1)).getFragments();
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentTransactions() throws Throwable {
        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentSchemas() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSchemaName("foo1");
        context1.setTableName("bar");
        context1.setDataSource("path");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setSchemaName("foo2");
        context2.setTableName("bar");
        context2.setDataSource("path");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentTables() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSchemaName("foo");
        context1.setTableName("bar1");
        context1.setDataSource("path");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setSchemaName("foo");
        context2.setTableName("bar2");
        context2.setDataSource("path");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentDataSources() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setSchemaName("foo");
        context1.setTableName("bar");
        context1.setDataSource("path1");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setSchemaName("foo");
        context2.setTableName("bar");
        context2.setDataSource("path2");
        context2.setFilterString("a3c25s10d2016-01-03o6");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void testFragmenterCallIsNotCachedForDifferentFilters() throws Throwable {
        context1.setTransactionId("XID-XYZ-123456");
        context1.setFilterString("a3c25s10d2016-01-03o6");

        context2.setTransactionId("XID-XYZ-123456");
        context2.setFilterString("a3c25s10d2016-01-03o2");

        testContextsAreNotCached(context1, context2);
    }

    @Test
    public void getSameFragmenterCallTwiceUsesCache() throws Throwable {
        List<Fragment> fragmentList = new ArrayList<>();

        context1.setTransactionId("XID-XYZ-123456");
        context2.setTransactionId("XID-XYZ-123456");

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(fragmenter1.getFragments()).thenReturn(fragmentList);

        List<Fragment> response1 = fragmenterService.getFragmentsForSegment(context1);
        List<Fragment> response2 = fragmenterService.getFragmentsForSegment(context2);

        verify(fragmenter1, times(1)).getFragments();

        assertEquals(fragmentList, response1);
        assertEquals(fragmentList, response2);
    }

    @Test
    public void testFragmenterCallExpiresAfterTimeout() throws Throwable {
        List<Fragment> fragmentList1 = new ArrayList<>();
        List<Fragment> fragmentList2 = new ArrayList<>();

        context1.setTransactionId("XID-XYZ-123456");
        context2.setTransactionId("XID-XYZ-123456");

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(mockPluginFactory.getPlugin(context2, context2.getFragmenter())).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        List<Fragment> response1 = fragmenterService.getFragmentsForSegment(context1);
        fakeTicker.advanceTime(11 * 1000);
        List<Fragment> response2 = fragmenterService.getFragmentsForSegment(context2);

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();
        // Checks for reference
        assertEquals(fragmentList1, response1);
        assertEquals(fragmentList2, response2);
    }

    @Test
    public void testMultiThreadedAccessToFragments() throws Throwable {
        final AtomicInteger finishedCount = new AtomicInteger();

        int threadCount = 100;
        Thread[] threads = new Thread[threadCount];
        final Fragmenter fragmenter = mock(Fragmenter.class);
        when(mockPluginFactory.getPlugin(any(), any())).thenReturn(fragmenter);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    fragmenterService.getFragmentsForSegment(context1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finishedCount.incrementAndGet();
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        verify(fragmenter, times(1)).getFragments();
        assertEquals(threadCount, finishedCount.intValue());

        // From the CacheBuilder documentation:
        // Expired entries may be counted in {@link Cache#size}, but will never be visible to read or
        // write operations. Expired entries are cleaned up as part of the routine maintenance described
        // in the class javadoc
        assertEquals(1, fragmentCache.size());
        // advance time one second force a cache clean up.
        // Cache retains the entry
        fakeTicker.advanceTime(1000);
        fragmentCache.cleanUp();
        assertEquals(1, fragmentCache.size());
        // advance 10 seconds and force a clean up
        // cache should be clean now
        fakeTicker.advanceTime(10 * 1000);
        fragmentCache.cleanUp();
        assertEquals(0, fragmentCache.size());
    }

    private void testContextsAreNotCached(RequestContext context1, RequestContext context2) throws Throwable {
        List<Fragment> fragmentList1 = new ArrayList<>();
        List<Fragment> fragmentList2 = new ArrayList<>();

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(mockPluginFactory.getPlugin(context2, context2.getFragmenter())).thenReturn(fragmenter2);

        when(fragmenter1.getFragments()).thenReturn(fragmentList1);
        when(fragmenter2.getFragments()).thenReturn(fragmentList2);

        List<Fragment> response1 = fragmenterService.getFragmentsForSegment(context1);
        List<Fragment> response2 = fragmenterService.getFragmentsForSegment(context2);

        verify(fragmenter1, times(1)).getFragments();
        verify(fragmenter2, times(1)).getFragments();

        assertEquals(fragmentList1, response1);
        assertEquals(fragmentList2, response2);

        assertEquals(2, fragmentCache.size());
        // advance time one second force a cache clean up.
        // Cache retains the entry
        fakeTicker.advanceTime(1000);
        fragmentCache.cleanUp();
        assertEquals(2, fragmentCache.size());
        // advance 10 seconds and force a clean up
        // cache should be clean now
        fakeTicker.advanceTime(10 * 1000);
        fragmentCache.cleanUp();
        assertEquals(0, fragmentCache.size());
    }

    private static class FakeTicker extends Ticker {

        static final int NANOS_PER_MILLIS = 1000000;
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        public void advanceTime(long milliseconds) {
            nanos.addAndGet(milliseconds * NANOS_PER_MILLIS);
        }
    }

    // ----- TESTS for operation retries due to 'GSS initiate failed' errors -----

    @Test
    public void testGetFragmentsFailureNoRetries() throws Throwable {
        configuration.set("hadoop.security.authentication", "kerberos");
        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter())).thenReturn(fragmenter1);
        when(fragmenter1.getFragments()).thenThrow(new IOException("Something Else"));

        Exception e = assertThrows(IOException.class, () -> fragmenterService.getFragmentsForSegment(context1));
        assertEquals("Something Else", e.getMessage());

        // verify got fragmenter and fragments only once
        verify(mockPluginFactory).getPlugin(context1, context1.getFragmenter());
        verify(fragmenter1).getFragments();
        verifyNoMoreInteractions(mockPluginFactory, fragmenter1);
    }

    @Test
    public void testGetFragmentsGSSFailureRetriedOnce() throws Throwable {
        List<Fragment> fragmentList = new ArrayList<>();
        configuration.set("hadoop.security.authentication", "kerberos");

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter()))
                .thenReturn(fragmenter1)
                .thenReturn(fragmenter2);
        when(fragmenter1.getFragments()).thenThrow(new IOException("GSS initiate failed"));
        when(fragmenter2.getFragments()).thenReturn(fragmentList);

        List<Fragment> result = fragmenterService.getFragmentsForSegment(context1);
        assertNotNull(result);

        // verify proper number of interactions
        verify(mockPluginFactory, times(2)).getPlugin(context1, context1.getFragmenter());
        InOrder inOrder = inOrder(fragmenter1, fragmenter2);
        inOrder.verify(fragmenter1).getFragments(); // first  attempt on fragmenter #1
        inOrder.verify(fragmenter2).getFragments(); // second attempt on fragmenter #2
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }

    @Test
    public void testBeginIterationGSSFailureRetriedTwice() throws Throwable {
        List<Fragment> fragmentList = new ArrayList<>();
        configuration.set("hadoop.security.authentication", "kerberos");

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter()))
                .thenReturn(fragmenter1)
                .thenReturn(fragmenter2)
                .thenReturn(fragmenter3);
        when(fragmenter1.getFragments()).thenThrow(new IOException("GSS initiate failed"));
        when(fragmenter2.getFragments()).thenThrow(new IOException("GSS initiate failed"));
        when(fragmenter3.getFragments()).thenReturn(fragmentList);

        List<Fragment> result = fragmenterService.getFragmentsForSegment(context1);
        assertNotNull(result);

        // verify proper number of interactions
        verify(mockPluginFactory, times(3)).getPlugin(context1, context1.getFragmenter());
        InOrder inOrder = inOrder(fragmenter1, fragmenter2, fragmenter3);
        inOrder.verify(fragmenter1).getFragments(); // first  attempt on fragmenter #1
        inOrder.verify(fragmenter2).getFragments(); // second attempt on fragmenter #2
        inOrder.verify(fragmenter3).getFragments(); // third  attempt on fragmenter #3
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }

    @Test
    public void testBeginIterationGSSFailureAfterMaxRetries() throws Throwable {
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("pxf.sasl.connection.retries", "2");

        when(mockPluginFactory.getPlugin(context1, context1.getFragmenter()))
                .thenReturn(fragmenter1)
                .thenReturn(fragmenter2)
                .thenReturn(fragmenter3);
        when(fragmenter1.getFragments()).thenThrow(new IOException("GSS initiate failed"));
        when(fragmenter2.getFragments()).thenThrow(new IOException("GSS initiate failed"));
        when(fragmenter3.getFragments()).thenThrow(new IOException("GSS initiate failed"));

        Exception e = assertThrows(IOException.class, () -> fragmenterService.getFragmentsForSegment(context1));
        assertEquals("GSS initiate failed", e.getMessage());

        // verify proper number of interactions
        verify(mockPluginFactory, times(3)).getPlugin(context1, context1.getFragmenter());
        InOrder inOrder = inOrder(fragmenter1, fragmenter2, fragmenter3);
        inOrder.verify(fragmenter1).getFragments(); // first  attempt on fragmenter #1
        inOrder.verify(fragmenter2).getFragments(); // second attempt on fragmenter #2
        inOrder.verify(fragmenter3).getFragments(); // third  attempt on fragmenter #3
        inOrder.verifyNoMoreInteractions();
        verifyNoMoreInteractions(mockPluginFactory);
    }
}