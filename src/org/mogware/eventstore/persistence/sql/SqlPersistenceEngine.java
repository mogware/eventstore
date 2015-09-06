package org.mogware.eventstore.persistence.sql;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;
import org.mogware.eventstore.CommitAttempt;
import org.mogware.eventstore.ConcurrencyException;
import org.mogware.eventstore.EventMessage;
import org.mogware.eventstore.Checkpoint;
import org.mogware.eventstore.Commit;
import org.mogware.eventstore.Snapshot;
import org.mogware.eventstore.LongCheckpoint;
import org.mogware.eventstore.DefaultSnapshot;
import org.mogware.eventstore.persistence.DefaultCommit;
import org.mogware.eventstore.persistence.DefaultStreamHead;
import org.mogware.eventstore.persistence.PersistStreams;
import org.mogware.eventstore.persistence.StreamHead;
import org.mogware.eventstore.persistence.StorageException;
import static org.mogware.eventstore.serialization.ExtensionMethods.deserialize;
import static org.mogware.eventstore.serialization.ExtensionMethods.serialize;
import org.mogware.eventstore.serialization.Serialize;
import org.mogware.system.Guid;

public class SqlPersistenceEngine implements PersistStreams {
    private static final String BucketId = "bucketid";
    private static final String StreamIdOriginal = "streamidoriginal";
    private static final String StreamRevision = "streamrevision";
    private static final String SnapshotRevision = "snapshotrevision";
    private static final String CommitId = "commitid";
    private static final String CommitSequence = "commitsequence";
    private static final String CommitStamp = "commitstamp";
    private static final String CheckpointNumber = "checkpointnumber";
    private static final String Headers = "headers";
    private static final String Payload = "payload";

    private final AtomicInteger initialized = new AtomicInteger(0);

    private final ConnectionFactory connectionFactory;
    private final SqlDialect dialect;
    private final Serialize serializer;
    private final Properties authority;
    private final int pageSize;
    private final StreamIdHasher streamIdHasher;

    public SqlPersistenceEngine(
            ConnectionFactory factory,
            SqlDialect dialect,
            Serialize serializer,
            Properties authority,
            int pageSize,
            StreamIdHasher streamIdHasher
    ) {
        if (factory == null)
            throw new NullPointerException("factory must not be null");
        if (dialect == null)
            throw new NullPointerException("dialect must not be null");
        if (serializer == null)
            throw new NullPointerException("serializer must not be null");
        if (authority == null)
            throw new NullPointerException("authority must not be null");
        if (streamIdHasher == null)
            throw new NullPointerException("streamIdHasher must not be null");
        if (pageSize < 0)
            throw new IllegalArgumentException("pageSize must be positive");
        this.connectionFactory = factory;
        this.dialect = dialect;
        this.serializer = serializer;
        this.authority = authority;
        this.pageSize = pageSize;
        this.streamIdHasher = streamIdHasher;
    }

    @Override
    public void initialize() {
        if (this.initialized.incrementAndGet() > 1)
            return;
        this.executeCommand((c, s) -> s.executeWithoutExceptions(
                this.dialect.getInitializeStorage()
        ));
    }

    @Override
    public Iterable<Commit> getFrom(String bucketId, LocalDateTime start) {
        return this.executeQuery((c, s) -> {
            s.addParameter(this.dialect.getBucketId(), bucketId);
            s.addParameter(this.dialect.getCommitStamp(), start);
            return () -> StreamSupport.stream(s.executePagedQuery(
                this.dialect.getGetCommitsFromInstant(), (p, q, r) -> { }
            ).spliterator(), false)
                .map((x) -> this.getCommit(x)).iterator();
        });
    }

    @Override
    public Iterable<Commit> getFrom(String checkpointToken) {
        LongCheckpoint checkpoint = LongCheckpoint.parse(checkpointToken);
        return this.executeQuery((c, s) -> {
            s.addParameter(this.dialect.getCheckpointNumber(),
                checkpoint.getLongValue());
            return () -> StreamSupport.stream(s.executePagedQuery(
                this.dialect.getGetCommitsFromCheckpoint(), (p, q, r) -> { }
            ).spliterator(), false)
                .map((x) -> this.getCommit(x)).iterator();
        });
    }

    @Override
    public Checkpoint getCheckpoint(String checkpointToken) {
        return LongCheckpoint.parse(checkpointToken);
    }

    @Override
    public Iterable<Commit> getFromTo(String bucketId,
            LocalDateTime start, LocalDateTime end) {
        return this.executeQuery((c, s) -> {
            s.addParameter(this.dialect.getBucketId(), bucketId);
            s.addParameter(this.dialect.getCommitStampStart(), start);
            s.addParameter(this.dialect.getCommitStampEnd(), end);
            return () -> StreamSupport.stream(s.executePagedQuery(
                this.dialect.getGetCommitsFromToInstant(), (p, q, r) -> { }
            ).spliterator(), false)
                .map((x) -> this.getCommit(x)).iterator();
        });
    }

    @Override
    public void purge() {
        this.executeCommand((c, s) -> s.executeNonQuery(
                this.dialect.getPurgeStorage()
        ));
    }

    @Override
    public void purge(String bucketId) {
        this.executeCommand((c, s) -> {
                s.addParameter(this.dialect.getBucketId(), bucketId);
                return s.executeNonQuery(dialect.getPurgeBucket());
        });
    }

    @Override
    public void drop() {
        this.executeCommand((c, s) -> s.executeNonQuery(
                this.dialect.getDrop()
        ));
    }

    @Override
    public void deleteStream(String bucketId, String streamId) {
        final String streamIdHash = this.streamIdHasher.GetHash(streamId);
        this.executeCommand((c, s) -> {
                s.addParameter(this.dialect.getBucketId(), bucketId);
                s.addParameter(this.dialect.getStreamId(), streamIdHash);
                return s.executeNonQuery(dialect.getDeleteStream());
        });
    }

    @Override
    public Iterable<Commit> getFrom(String bucketId, String streamId,
            int minRevision, int maxRevision) {
        final String streamIdHash = this.streamIdHasher.GetHash(streamId);
        return this.executeQuery((c, s) -> {
            s.addParameter(this.dialect.getBucketId(), bucketId);
            s.addParameter(this.dialect.getStreamId(), streamIdHash);
            s.addParameter(this.dialect.getStreamRevision(), minRevision);
            s.addParameter(this.dialect.getMaxStreamRevision(), maxRevision);
            s.addParameter(this.dialect.getCommitSequence(), 0);
            return () -> StreamSupport.stream(s.executePagedQuery(
                this.dialect.getGetCommitsFromStartingRevision(),
                this.dialect.getNextPageDelegate()
            ).spliterator(), false)
                .map((x) -> this.getCommit(x)).iterator();
        });
    }

    private Commit getCommit(DataRecord record) {
        Map<String, Object> headers = deserialize(
                this.serializer, (byte[]) record.get(Headers)
        );
        List<EventMessage> events = deserialize(
                this.serializer, (byte[]) record.get(Payload)
        );
        return new DefaultCommit(
            (String) record.get(BucketId),
            (String) record.get(StreamIdOriginal),
            (int) record.get(StreamRevision),
            Guid.valueOf((byte[]) record.get(CommitId)),
            (int) record.get(CommitSequence),
            (LocalDateTime) record.get(CommitStamp),
            new LongCheckpoint((long) record.get(CheckpointNumber)).getValue(),
            headers,
            events
        );
    }

    @Override
    public Commit commit(CommitAttempt attempt) {
        Commit commit;
        try {
            commit = persistCommit(attempt);
        } catch (Exception ex) {
            throw new ConcurrencyException(ex.getMessage());
        }
        return commit;
    }

    @Override
    public Snapshot getSnapshot(String bucketId, String streamId,
            int maxRevision) {
        final String streamIdHash = this.streamIdHasher.GetHash(streamId);
        return (Snapshot) StreamSupport.stream(this.executeQuery((c, s) -> {
            s.addParameter(this.dialect.getBucketId(), bucketId);
            s.addParameter(this.dialect.getStreamId(), streamIdHash);
            s.addParameter(this.dialect.getStreamRevision(), maxRevision);
            return () -> StreamSupport.stream(s.executeWithQuery(
                this.dialect.getGetSnapshot()
            ).spliterator(), false)
                .map((x) -> this.getSnapshot(x, streamId)).iterator();
        }).spliterator(), false).findFirst().orElse(null);
    }

    private Snapshot getSnapshot(DataRecord record, String streamId) {
        return new DefaultSnapshot(
            (String) record.get(BucketId),
            streamId,
            (int) record.get(StreamRevision),
            null
        );
    }

    @Override
    public boolean addSnapshot(Snapshot snapshot) {
        final String streamIdHash =
                this.streamIdHasher.GetHash(snapshot.getStreamId());
        return this.executeCommand((c, s) -> {
            s.addParameter(this.dialect.getBucketId(), snapshot.getBucketId());
            s.addParameter(this.dialect.getStreamId(), streamIdHash);
            s.addParameter(this.dialect.getStreamRevision(),
                    snapshot.getStreamRevision());
            this.dialect.addPayloadParamater(this.connectionFactory, c, s,
                    serialize(this.serializer, snapshot.getPayload())
            );
            return s.executeWithoutExceptions(
                this.dialect.getAppendSnapshotToCommit()
            );
        }) > 0;
    }

    @Override
    public Iterable<StreamHead> getStreamsToSnapshot(String bucketId,
            int maxThreshold) {
        return this.executeCommand((c, s) -> {
            s.addParameter(this.dialect.getBucketId(), bucketId);
            s.addParameter(this.dialect.getThreshold(), maxThreshold);
            return () -> StreamSupport.stream(s.executePagedQuery(
                this.dialect.getGetStreamsRequiringSnapshots(), (p, q, r) -> { }
            ).spliterator(), false)
                .map((x) -> this.getStreamToSnapshot(x)).iterator();
        });
    }

    private StreamHead getStreamToSnapshot(DataRecord record) {
        return new DefaultStreamHead(
            (String) record.get(BucketId),
            (String) record.get(StreamIdOriginal),
            (int) record.get(StreamRevision),
            (int) record.get(SnapshotRevision)
        );
    }

    public Commit persistCommit(CommitAttempt attempt) {
        final String streamId = this.streamIdHasher.GetHash(
                attempt.getStreamId()
        );
        return this.executeCommand((c, s) -> {
            s.addParameter(this.dialect.getBucketId(), attempt.getBucketId());
            s.addParameter(this.dialect.getStreamId(), streamId);
            s.addParameter(this.dialect.getStreamIdOriginal(),
                    attempt.getStreamId());
            s.addParameter(this.dialect.getCommitId(), attempt.getCommitId());
            s.addParameter(this.dialect.getCommitSequence(),
                    attempt.getCommitSequence());
            s.addParameter(this.dialect.getStreamRevision(),
                    attempt.getStreamRevision());
            s.addParameter(this.dialect.getItems(), attempt.getEvents().size());
            s.addParameter(this.dialect.getCommitStamp(),
                    attempt.getCommitStamp());
            s.addParameter(this.dialect.getHeaders(),
                    serialize(this.serializer, attempt.getHeaders())
            );
            this.dialect.addPayloadParamater(this.connectionFactory, c, s,
                    serialize(this.serializer, attempt.getEvents())
            );
            int checkpoint = s.executeNonQuery(
                    this.dialect.getPersistCommit()
            );
            return new DefaultCommit(
                attempt.getBucketId(),
                attempt.getStreamId(),
                attempt.getStreamRevision(),
                attempt.getCommitId(),
                attempt.getCommitSequence(),
                attempt.getCommitStamp(),
                Integer.toString(checkpoint),
                attempt.getHeaders(),
                attempt.getEvents()
            );
        });
    }

    private <T> Iterable<T> executeQuery(Command<Iterable<T>> query) {
        try {
            String url = dialect.getConnectionURL(this.authority);
            Connection connection = this.connectionFactory.open(url,
                    this.authority.getProperty("user"),
                    this.authority.getProperty("password")
            );
            SqlCommand command = dialect.buildCommand(connection);
            command.setPageSize(this.pageSize);
            return query.apply(connection, command);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    private <T> T executeCommand(Command<T> command) {
        try {
            String url = dialect.getConnectionURL(this.authority);
            Connection connection = this.connectionFactory.open(url,
                    this.authority.getProperty("user"),
                    this.authority.getProperty("password")
            );
            return command.apply(connection, dialect.buildCommand(connection));
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }
}
