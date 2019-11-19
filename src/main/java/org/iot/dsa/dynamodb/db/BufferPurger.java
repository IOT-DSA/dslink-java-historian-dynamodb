package org.iot.dsa.dynamodb.db;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.etsdb.db.DbPurger;
import org.etsdb.TimeRange;
import org.etsdb.impl.DatabaseImpl;
import org.iot.dsa.dynamodb.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPurger {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbPurger.class);
    private Map<DatabaseImpl<?>, PurgeSettings> databases = new HashMap<DatabaseImpl<?>, PurgeSettings>();
    private ScheduledFuture<?> fut;
    private boolean running;

    public void addDb(DatabaseImpl<?> db, PurgeSettings purgeSettings) {
        synchronized(databases) {
            if (!databases.containsKey(db)) {
                databases.put(db, purgeSettings);
            }
        }
    }

    public void removeDb(DatabaseImpl<?> db) {
        synchronized (databases) {
            databases.remove(db);
        }
    }

    public void stop() {
        running = false;
        synchronized (this) {
            if (fut != null) {
                fut.cancel(true);
            }
        }
    }

    public void setupPurger() {
        running = true;
        Runnable runner = new Runnable() {
            @Override
            public void run() {
                synchronized (databases) {
                    for (Entry<DatabaseImpl<?>, PurgeSettings> entry : databases.entrySet()) {
                        DatabaseImpl<?> db = entry.getKey();
                        PurgeSettings settings = entry.getValue();
                        if (!(settings.isPurgeEnabled() && running)) {
                            continue;
                        }

    
                        File path = db.getBaseDir();
                        long currSize = FileUtils.sizeOf(path);
                        long maxSize = settings.getMaxSizeInBytes();
                        long delCount = 0;

                        if (maxSize - currSize <= 0) {
                            if (!running) {
                                break;
                            }
                            List<String> series = Util.getSanitizedSeriesIds(db);
                            while (maxSize - currSize <= 0) {
                                TimeRange range = db.getTimeRange(series);
                                if (range == null || range.isUndefined()) {
                                    break;
                                }
                                long from = range.getFrom();
                                for (String s : series) {
                                    delCount += db.delete(s, from, from + 3600000);
                                }
                                if (delCount <= 0) {
                                    break;
                                }
                                currSize = FileUtils.sizeOf(path);
                            }
                        }
                        if (delCount > 0) {
//                            String p = path.getPath();
//                            LOGGER.info("Deleted {} records from {}", delCount, p);
                        }
                    }
                }
            }
        };
        ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
        synchronized (this) {
            TimeUnit u = TimeUnit.SECONDS;
            fut = stpe.scheduleWithFixedDelay(runner, 30, 30, u);
        }
    }
}
