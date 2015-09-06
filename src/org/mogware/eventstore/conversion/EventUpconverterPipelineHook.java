package org.mogware.eventstore.conversion;

import java.util.Map;
import java.util.function.Function;
import org.mogware.eventstore.PipelineHookBase;

public class EventUpconverterPipelineHook extends PipelineHookBase {
    private final Map<Class,Function<Object, Object>> converters;

    public EventUpconverterPipelineHook(
            Map<Class,Function<Object, Object>> converters) {
        if (converters == null)
            throw new NullPointerException("converters must not be null");
        this.converters = converters;
    }
}
