package org.logstashplugins;

import co.elastic.logstash.api.*;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Metadata;
import com.maxmind.db.NoCache;
import com.maxmind.db.Reader;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// class name must match plugin name
@LogstashPlugin(name = "mmdb")
public class MMDB implements Filter {

    public static final PluginConfigSpec<String> SOURCE_CONFIG =
        PluginConfigSpec.requiredStringSetting("source");
    public static final PluginConfigSpec<String> TARGET_CONFIG =
        PluginConfigSpec.requiredStringSetting("target");
    public static final PluginConfigSpec<String> DATABASE_FILENAME_CONFIG =
        PluginConfigSpec.requiredStringSetting("database");
    public static final PluginConfigSpec<Long> CACHE_SIZE_CONFIG =
        PluginConfigSpec.numSetting("cache_size", 0L);
    public static final PluginConfigSpec<List<Object>> FIELDS_CONFIG =
        PluginConfigSpec.arraySetting("fields");


    private final static AtomicReference<Reader> readerRef = new AtomicReference<>();
    private static WatchService watchService;
    public static long lastModifiedTime = 0L;

    private String id;
    private String sourceField;
    private String targetField;
    private String databaseFilename;
    private String failureTag = "_mmdb_lookup_failure";
    private Map<String, FieldNode> fieldNodeMap;
    private int cacheSize = 0;

    private static final Pattern FIELD_PATTERN = Pattern.compile("(?<before>\\w+(\\.\\w+)*)(\\s*:\\s*(?<after>\\w+))?");

    public MMDB(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
        this.targetField = config.get(TARGET_CONFIG);
        this.databaseFilename = config.get(DATABASE_FILENAME_CONFIG);
        this.fieldNodeMap = null; // null = all fields to be exported

        if (this.databaseFilename == null) {
            throw new IllegalStateException("Must specify database filename");
        }

        if (this.sourceField == null) {
            throw new IllegalStateException("Must specify source field");
        }

        if (this.targetField == null) {
            throw new IllegalStateException("Must specify target field");
        }

        List<Object> fieldsTmp = config.get(FIELDS_CONFIG);
        if (fieldsTmp != null) {
            this.fieldNodeMap = new HashMap<>();
            for (Object o : fieldsTmp) {
                if (o instanceof String) {
                    Matcher matcher;
                    if ((matcher = FIELD_PATTERN.matcher((String) o)).matches()) {
                        String before = matcher.group("before");
                        String after_tmp = matcher.group("after");
                        String after = after_tmp == null || after_tmp.isEmpty() ? before : after_tmp;
                        //split every nested filed like ["a","a.b.c:bb","e.f:dd"]
                        String[] fs = before.split("\\.");
                        Map<String, FieldNode> fieldNodeMap = this.fieldNodeMap;
                        for (int i = 0; i < fs.length; i++) {
                            FieldNode node = fieldNodeMap.get(fs[i]);
                            if (node == null) {
                                node = new FieldNode(fs[i], i == fs.length - 1 ? after : null);
                                fieldNodeMap.put(fs[i], node);
                            } else if (i == fs.length - 1) {
                                node.setTarget(after);
                            }
                            fieldNodeMap = fieldNodeMap.get(fs[i]).getChildMap();
                        }
                        continue;
                    }
                }
                throw new IllegalStateException("Fields config must only be a list of strings");
            }
        }

        this.cacheSize = config.get(CACHE_SIZE_CONFIG).intValue();
        if(this.cacheSize < 0) {
            throw new IllegalStateException("Cache size must be either >0 to use a cache, or =0 to use no cache");
        }

        File databaseFile = new File(this.databaseFilename);
        try {
            Reader databaseReader = new Reader(databaseFile, cacheSize > 0 ? new CHMCache(cacheSize) : NoCache.getInstance());
            readerRef.set(databaseReader);
            watchService = FileSystems.getDefault().newWatchService();
            Path filePath = databaseFile.toPath();
            filePath.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            Executors.newSingleThreadExecutor().execute(() -> {
                while (true) {
                    try {
                        WatchKey watchKey = watchService.take();
                        if (watchKey != null) {
                            List<WatchEvent<?>> events = watchKey.pollEvents();
                            for (WatchEvent<?> event : events) {
                                if (event.kind() == StandardWatchEventKinds.OVERFLOW ||
                                    ((WatchEvent<Path>) event).context().equals(filePath.getFileName())) {
                                    try {
                                        Reader old = readerRef.get();
                                        Reader reader = new Reader(databaseFile, cacheSize > 0 ? new CHMCache(cacheSize) : NoCache.getInstance());
                                        readerRef.set(reader);
                                        old.close();
                                        lastModifiedTime = System.currentTimeMillis();
                                        context.getLogger(MMDB.this).info("mmdb reload:" + reader.getMetadata().toString());
                                    } catch (Exception ex) {
                                        context.getLogger(MMDB.this).error("mmdb reload failed", ex);
                                    }
                                }
                            }
                            boolean valid = watchKey.reset();
                            if (!valid) {
                                break;
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    } catch (Throwable e) {
                        context.getLogger(MMDB.this).error("mmdb watch error", e);
                    }
                }
            });
        } catch (java.io.IOException ex) {
            throw new IllegalStateException("Database does not appear to be a valid database", ex);
        }
        context.getLogger(this).info(readerRef.get().getMetadata().toString());
    }

    public Metadata getMetadata() {
        if (null == readerRef.get()) {
            return null;
        }
        return readerRef.get().getMetadata();
    }

    // This assumes that the fields in the MMDB are a flat structure
    private void renderMapIntoEvent(Map<String, FieldNode> fieldNodeMap,
                                    Map<String, Object> data,
                                    Event e) {
        if (fieldNodeMap != null && !fieldNodeMap.isEmpty()) {
            //support nested map
            for (Map.Entry<String, FieldNode> entry : fieldNodeMap.entrySet()) {
                FieldNode fieldNode = entry.getValue();
                Object value = data.get(fieldNode.getName());
                if (value != null) {
                    if (fieldNode.getTarget() != null) {
                        setField(e, fieldNode.getTarget(), value);
                    }
                    if (fieldNode.getChildMap() != null && !fieldNode.getChildMap().isEmpty()
                        && value instanceof Map) {
                        renderMapIntoEvent(fieldNode.getChildMap(), (Map<String, Object>) value, e);
                    }
                }
            }
        } else {
            for (Map.Entry<String, Object> field : data.entrySet()) {
                setField(e, field.getKey(), field.getValue());
            }
        }
    }

    private void setField(Event e, String key, Object value) {
        key = "[" + this.targetField + "][" + key + "]";
        if (value instanceof String) {
            e.setField(key, value);
        } else if (value instanceof Long) {
            e.setField(key, value);
        } else if (value instanceof Float) {
            e.setField(key, value);
        } else if (value instanceof Boolean) {
            e.setField(key, value);
        }
        //support nested map or list
        else if (value instanceof Map
            || value instanceof List) {
            e.setField(key, value);
        }

        // FIXME: Should we support lists and objects?
        else {
            e.tag(this.failureTag);
        }
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        for (Event e : events) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> recordData = readerRef.get().get(
                    InetAddress.getByName(
                        e.getField(this.sourceField).toString()),
                    Map.class);

                if (null == recordData) {
                    e.tag(this.failureTag);
                    continue;
                }

                renderMapIntoEvent(this.fieldNodeMap, recordData, e);

                matchListener.filterMatched(e);

            } catch (java.net.UnknownHostException ex) {
                e.tag(this.failureTag);
                continue;
            } catch (IOException ex) {
                e.tag(this.failureTag);
                continue;
            }
        }
        return events;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {

        // The Java example I was looking at doesn't tell
        // you that you need to include the common config
        // too, nor does it show how.
        // 
        // This form of commonFilterSettings, with an
        // argument, will merge the provided settings with
        // the common ones for filter.
        //
        // Note that the checking of arguments is not done
        // when we run the unit-tests; that's not our
        // code. You may therefore encounter this during
        // integration testing instead.

        return PluginHelper.commonFilterSettings(
            Arrays.asList(
                SOURCE_CONFIG,
                TARGET_CONFIG,
                DATABASE_FILENAME_CONFIG,
                CACHE_SIZE_CONFIG,
                FIELDS_CONFIG));
    }

    @Override
    public String getId() {
        return this.id;
    }
}
