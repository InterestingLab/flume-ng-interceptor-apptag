package org.interestinglab.flume.interceptor;

import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Interceptor class that appends a static, pre-configured header to all events.
 *
 * Properties:<p>
 *
 *   key: Key to use in static header insertion.
 *        (default is "key")<p>
 *
 *   value: Value to use in static header insertion.
 *        (default is "value")<p>
 *
 *   preserveExisting: Whether to preserve an existing value for 'key'
 *                     (default is true)<p>
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = static<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = false<p>
 *   agent.sources.r1.interceptors.i1.key = datacenter<p>
 *   agent.sources.r1.interceptors.i1.value= NYC_01<p>
 * </code>
 *
 */
public class AppTagInterceptor implements Interceptor {
    
    private static final Logger logger = LoggerFactory.getLogger(AppTagInterceptor.class);
    
    private final boolean preserveTag;
    private final String header;
    private final String defaultTag;
    
    private AppTagInterceptor(boolean preserveTag, String header, String defaultTag) {
        this.preserveTag = preserveTag;
        this.header = header;
        this.defaultTag = defaultTag;
    }

    @Override
    public void close()
    {

    }

    @Override
    public void initialize()
    {

    }

    @Override
    public Event intercept(Event event)
    {        
        Map<String, String> headers = event.getHeaders();
        
        String eventBody = new String(event.getBody(), UTF_8);
        eventBody = eventBody.trim();
        
        int index = eventBody.indexOf(' ');
            
        if (index == -1) {
            headers.put(this.header, this.defaultTag);
        }
        else {
            String appTag = eventBody.substring(0, index);
            headers.put(this.header, appTag);
        }

        if (this.preserveTag == false) {
            String newEventBody = eventBody.substring(index, eventBody.length()).trim();
            event.setBody(newEventBody.getBytes());
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events)
    {
        for (Event event : events){

            intercept(event);
        }

        return events;
    }

    public static class Builder implements Interceptor.Builder
    {
        private boolean preserveTag;
        private String header;
        private String defaultTag;
        
        @Override
        public void configure(Context context) {
            preserveTag = context.getBoolean(Constants.PRESERVE, Constants.PRESERVE_DEFAULT);
            header = context.getString(Constants.HEADER, Constants.HEADER_DEFAULT);
            defaultTag = context.getString(Constants.DEFAULT_TAG, Constants.DEFAULT_TAG_DEFAULT);
        }

        @Override
        public Interceptor build() {
            logger.info(String.format(
                "Creating AppTagInterceptor: preserveTag=%s,header=%s,defaultTag=%s",
                preserveTag, header, defaultTag));

            return new AppTagInterceptor(preserveTag, header, defaultTag);
        }
    }
    
    public static class Constants {
        public static final String HEADER = "header";
        public static final String HEADER_DEFAULT = "app_tag";
        
        public static final String DEFAULT_TAG = "defaultTag";
        public static final String DEFAULT_TAG_DEFAULT = "myapp";

        public static final String PRESERVE = "preserveTag";
        public static final boolean PRESERVE_DEFAULT = true;
    }
}
