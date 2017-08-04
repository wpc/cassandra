package org.apache.cassandra.rocksdb.tools;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * Compare the rowiterators from Rocksdb and Cassandra and record the consistency.
 */
public class RowIteratorSanityCheck
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SanityCheckUtils.class);

    private long partitions;
    private long cassandraMissingPartitions;
    private long rocksdbMissingPartitions;
    private long mismatcPartitions;
    private long partitionDeletionMismatch;
    private long rangeTombstoneSkipped;

    private long rows;
    private long cassandraMissingRows;
    private long rocksdbMissingRows;
    private long mismatchRows;

    public RowIteratorSanityCheck()
    {
        partitions = 0;
        cassandraMissingPartitions = 0;
        rocksdbMissingPartitions = 0;
        mismatcPartitions = 0;
        partitionDeletionMismatch = 0;
        rangeTombstoneSkipped = 0;
        rows = 0;
        cassandraMissingRows = 0;
        rocksdbMissingRows = 0;
        mismatchRows = 0;
    }

    public void compare(UnfilteredRowIterator cassandraPartition, UnfilteredRowIterator rocksdbPartition)
    {
        partitions++;
        if (cassandraPartition == null && rocksdbPartition == null)
            return;

        if (cassandraPartition == null)
        {
            cassandraMissingPartitions++;
            return;
        }

        if (rocksdbPartition == null)
        {
            rocksdbMissingPartitions++;
            return;
        }
        assert (cassandraPartition.partitionKey().equals(rocksdbPartition.partitionKey()));
        partitionDeletionMismatch += cassandraPartition.partitionLevelDeletion().equals(rocksdbPartition.partitionLevelDeletion()) ? 0 : 1;

        boolean hasRowTombstone = false;
        Map<Clustering, Row> cassandraRows = new HashMap<>();
        while (cassandraPartition.hasNext())
        {
            Unfiltered unfilterd = cassandraPartition.next();
            if (unfilterd.isRangeTombstoneMarker())
            {
                hasRowTombstone = true;
                continue;
            }
            else
            {
                Row row = (Row) unfilterd;
                cassandraRows.put(row.clustering(), row);
            }
        }

        Map<Clustering, Row> rocksdbRows = new HashMap<>();
        while (rocksdbPartition.hasNext())
        {
            // We don't support range tombstone in RocksEngine yet.
            Row row = (Row) rocksdbPartition.next();
            LOGGER.info(row.toString());
            rocksdbRows.put(row.clustering(), row);
        }

        rangeTombstoneSkipped += hasRowTombstone ? 1 : 0;
        if (!compare(cassandraRows, rocksdbRows))
        {
            mismatcPartitions++;
        }
    }

    public boolean compare(Map<Clustering, Row> cassandraRows, Map<Clustering, Row> rocksdbRows)
    {
        Set<Clustering> clusterings = new HashSet<>(cassandraRows.keySet());
        clusterings.addAll(rocksdbRows.keySet());
        rows += clusterings.size();
        boolean match = true;
        for (Clustering c : clusterings)
        {
            if (!cassandraRows.containsKey(c))
            {
                cassandraMissingRows++;
                match = false;
            }
            else if (!rocksdbRows.containsKey(c))
            {
                rocksdbMissingRows++;
                match = false;
            }
            else
            {
                Row cassandraRow = cassandraRows.get(c);
                Row rocksdbRow = rocksdbRows.get(c);
                if (!digest(cassandraRow).equals(digest(rocksdbRow)))
                {
                    match = false;
                    mismatchRows++;
                }
            }
        }
        return match;
    }

    private String digest(Row row)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            row.digest(digest);
            return digest.toString();
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("Digest failed:" + e.getMessage());
        }
    }

    public Report getReport()
    {
        return new Report(this);
    }

    @Override
    public String toString()
    {
        return getReport().toString();
    }

    public static class Report
    {
        public long partitions;
        public long cassandraMissingPartitions;
        public long rocksdbMissingPartitions;
        public long mismatcPartitions;
        public long partitionDeletionMismatch;
        public long rangeTombstoneSkipped;

        public long rows;
        public long cassandraMissingRows;
        public long rocksdbMissingRows;
        public long mismatchRows;

        public Report(RowIteratorSanityCheck comparator)
        {
            this.partitions = comparator.partitions;
            this.cassandraMissingPartitions = comparator.cassandraMissingPartitions;
            this.rocksdbMissingPartitions = comparator.rocksdbMissingPartitions;
            this.mismatcPartitions = comparator.mismatcPartitions;
            this.partitionDeletionMismatch = comparator.partitionDeletionMismatch;
            this.rangeTombstoneSkipped = comparator.rangeTombstoneSkipped;
            this.rows = comparator.rows;
            this.cassandraMissingRows = comparator.cassandraMissingRows;
            this.rocksdbMissingRows = comparator.rocksdbMissingRows;
            this.mismatchRows = comparator.mismatchRows;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("Sanity check result:")
              .append("\n  total partitions: ").append(partitions)
              .append("\n    cassandra missing partitions: ").append(cassandraMissingPartitions)
              .append("\n    rocksdb missing partitions: ").append(rocksdbMissingPartitions)
              .append("\n    mismatch partitions: ").append(mismatcPartitions)
              .append("\n    mismatch partition deletions: ").append(partitionDeletionMismatch)
              .append("\n    skipped range tombstones: ").append(rangeTombstoneSkipped)
              .append("\n    total rows: ").append(rows)
              .append("\n    cassandra missing rows: ").append(cassandraMissingRows)
              .append("\n    rocksdb missing rows: ").append(rocksdbMissingRows)
              .append("\n    mismatched rows: ").append(mismatchRows);
            return sb.toString();
        }
    }

}
