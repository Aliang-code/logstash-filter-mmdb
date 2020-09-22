package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.FilterMatchListener;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.ContextImpl;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import org.junit.Test;

import java.util.List;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.maxmind.db.Metadata;

public class MMDBTest {

    @Test
    public void testConfigRequiresDatabase() {
        
        HashMap configMap = new HashMap();
        configMap.put("source", "ip");
        configMap.put("target", "info");
        Configuration config = new ConfigurationImpl(configMap);
        Context context = new ContextImpl(null, null);

        try {
            MMDB filter = new MMDB("test-id", config, context);
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("Must specify database filename"));
        }
    }

    @Test
    public void testRequiresValidDatabase() {
        
        HashMap configMap = new HashMap();
        configMap.put("source", "ip");
        configMap.put("target", "info");
        configMap.put("database", "samples/demo.mmdb");
        Configuration config = new ConfigurationImpl(configMap);
        Context context = new ContextImpl(null, null);

        MMDB filter = new MMDB("test-id", config, context);
        assertThat(filter.getMetadata().getIpVersion(), is(4));
        assertThat(filter.getMetadata().getDatabaseType(), is("demo-network"));        
    }

    @Test
    public void testDemoBasic() {
        
        HashMap configMap = new HashMap();
        configMap.put("source", "ip");
        configMap.put("target", "info");
        configMap.put("database", "samples/demo.mmdb");
        Configuration config = new ConfigurationImpl(configMap);
        Context context = new ContextImpl(null, null);
        MMDB filter = new MMDB("test-id", config, context);

        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        e.setField("ip", "172.16.0.1");
        Collection<Event> results = filter.filter(Collections.singletonList(e), matchListener);

        assertNull(e.getField("tags"));
        assertThat(e.getField("[ip]"), is("172.16.0.1"));
        assertThat(e.getField("[info][subnet]"), is("172.16.0.0/12"));
        assertThat(e.getField("[info][name]"), is("DMZ"));
        assertThat(e.getField("[info][vlan_id]"), is(234L));
    }

    @Test
    public void testDemoBasicUnicode() {
        
        HashMap configMap = new HashMap();
        configMap.put("source", "ip");
        configMap.put("target", "info");
        configMap.put("database", "samples/demo.mmdb");
        Configuration config = new ConfigurationImpl(configMap);
        Context context = new ContextImpl(null, null);
        MMDB filter = new MMDB("test-id", config, context);

        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        e.setField("ip", "10.64.1.255");
        Collection<Event> results = filter.filter(Collections.singletonList(e), matchListener);

        assertNull(e.getField("tags"));
        assertThat(e.getField("[ip]"), is("10.64.1.255"));
        assertThat(e.getField("[info][subnet]"), is("10.64.0.0/23"));
        assertThat(e.getField("[info][name]"), is("Unicode NFKC Test (ﬃ)"));
        assertThat(e.getField("[info][vlan_id]"), is(234L));
    }
}

class TestMatchListener implements FilterMatchListener {

    private AtomicInteger matchCount = new AtomicInteger(0);

    @Override
    public void filterMatched(Event event) {
        matchCount.incrementAndGet();
    }

    public int getMatchCount() {
        return matchCount.get();
    }
}