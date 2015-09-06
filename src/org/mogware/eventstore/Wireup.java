package org.mogware.eventstore;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.mogware.eventstore.conversion.EventUpconverterPipelineHook;
import org.mogware.eventstore.persistence.PersistStreams;
import org.mogware.eventstore.persistence.PersistenceWireup;
import org.mogware.eventstore.conversion.EventUpconverterWireup;
import org.mogware.eventstore.persistence.memory.InMemoryPersistenceEngine;
import org.mogware.system.delegates.Func1;
import org.mogware.system.ioc.Container;

public class Wireup {
    private final Container container;
    private final Wireup inner;

    protected Wireup(Container container) {
        this.container = container;
        this.inner = null;
    }

    protected Wireup(Wireup inner) {
        this.inner = inner;
        this.container = null;
    }

    protected Container getContainer() {
        return this.container != null ?
            this.container : this.inner.getContainer();
    }

    public static Wireup init() {
        Container container = new Container();
        container.register(TransactionScopeOption.class,
                TransactionScopeOption.Suppress);
        container.register(EventStore.class, buildEventStore);
        return new Wireup(container);
    }

    public Wireup with(Class service, Object instance) {
        this.getContainer().register(service, instance);
        return this;
    }

    public PersistenceWireup usingInMemoryPersistence() {
        this.with(PersistStreams.class, new InMemoryPersistenceEngine());
        return new PersistenceWireup(this);
    }

    public EventUpconverterWireup usingEventUpconversion() {
        return new EventUpconverterWireup(this);
    }

    public Wireup hookIntoPipelineUsing(List<PipelineHook> hooks) {
        return hookIntoPipelineUsing(hooks != null ?
                hooks.toArray(new PipelineHook[0]) : new PipelineHook[0]
        );
    }

    public Wireup hookIntoPipelineUsing(PipelineHook[] hooks) {
        getContainer().register(PipelineHook[].class,
                hooks != null ? hooks : new PipelineHook[0]);
        return this;
    }

    public EventStore build() {
        if (this.inner != null)
            return this.inner.build();
        return getContainer().resolve(EventStore.class);
    }

    private static Func1<Container, Object> buildEventStore = (context) -> {
        System.out.println("build from Wireup");
        TransactionScopeOption scopeOption = 
                context.resolve(TransactionScopeOption.class);
        OptimisticPipelineHook concurrency =
                scopeOption == TransactionScopeOption.Suppress ?
                        new OptimisticPipelineHook() : null;
        EventUpconverterPipelineHook upconverter =
                context.resolve(EventUpconverterPipelineHook.class);

        PipelineHook[] hooks = context.resolve(PipelineHook[].class);

        hooks = Stream.concat(
                Arrays.asList(concurrency, upconverter).stream(),
                Arrays.stream(hooks))
                    .filter((x) -> x != null)
                    .toArray(size -> new PipelineHook[size]);

        return new OptimisticEventStore(
                context.resolve(PersistStreams.class), hooks);
    };
}
