package org.greenplum.pxf.plugins.jdbc.writercallable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TimeoutFixedThreadPoolExecutor extends ThreadPoolExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TimeoutFixedThreadPoolExecutor.class);
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentMap<Runnable, ScheduledFuture<?>> runningTasks = new ConcurrentHashMap<>();

    public TimeoutFixedThreadPoolExecutor(int poolSize, long timeout, TimeUnit timeoutUnit) {
        super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    @Override
    public void shutdown() {
        timeoutExecutor.shutdown();
        super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        timeoutExecutor.shutdownNow();
        return super.shutdownNow();
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
            final ScheduledFuture<?> scheduled = timeoutExecutor.schedule(new TimeoutTask(t), timeout, timeoutUnit);
            LOG.trace("Thread {} will time out in {} {}", t.getName(), timeout, timeoutUnit);
            runningTasks.put(r, scheduled);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        ScheduledFuture<?> timeoutTask = runningTasks.remove(r);
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
    }

    class TimeoutTask implements Runnable {
        private final Thread thread;

        public TimeoutTask(Thread thread) {
            this.thread = thread;
        }

        @Override
        public void run() {
            thread.interrupt();
            LOG.warn("Thread {} has timed out", thread.getName());
        }
    }
}
