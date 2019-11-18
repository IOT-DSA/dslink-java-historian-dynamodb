package org.iot.dsa.dynamodb.db;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.etsdb.db.Db;
import org.dsa.iot.etsdb.db.DbPurger;
import org.dsa.iot.etsdb.serializer.ByteData;
import org.etsdb.Database;
import org.etsdb.TimeRange;
import org.etsdb.impl.DatabaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPurger {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbPurger.class);
    private Map<Database<?>, PurgeSettings> databases = new HashMap<Database<?>, PurgeSettings>();
    private ScheduledFuture<?> fut;
    private boolean running;

    public void addDb(Database<?> db, PurgeSettings purgeSettings) {
        synchronized(databases) {
            if (!databases.containsKey(db)) {
                databases.put(db, purgeSettings);
            }
        }
    }

    public void removeDb(Db db) {
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
                    for (Entry<Database<?>, PurgeSettings> entry : databases.entrySet()) {
                        Database<?> db = entry.getKey();
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
                            List<String> series = db.getSeriesIds();
                            if (File.separatorChar != '/') {
                                List<String> corrected = new ArrayList<String>();
                                for (String s: series) {
                                    corrected.add(s.replace(File.separatorChar, '/'));
                                }
                                series = corrected;
                            }
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
