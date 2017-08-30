package org.apache.cassandra.rocksdb.tools;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

public class SanityCheckUtils
{

    private static final Logger LOGGER = LoggerFactory.getLogger(SanityCheckUtils.class);

    /**
     * Issue a full partition read to the Cassandra storage engine of a given cfs, then for each partion compare the result with the one from Rocksdb Engine.
     * Doesn't work with expring data.
     *
     * @param cfs ColumnFamilyStore which has double write enabled.
     * @param randomStartToken Choose a random token in the ring to start with instead of minimal token.
     * @param limit number of rows to check, 0 means unlimited.
     * @return Comparator Report.
     */
    public static RowIteratorSanityCheck.Report checkSanity(ColumnFamilyStore cfs, boolean randomStartToken, long limit)
    {
        Token fromToken = randomStartToken ? cfs.metadata.partitioner.getRandomToken() : cfs.metadata.partitioner.getMinimumToken();
        InternalPartitionRangeReadCommand command = new InternalPartitionRangeReadCommand((PartitionRangeReadCommand) (new AbstractReadCommandBuilder.PartitionRangeBuilder(cfs).fromToken(fromToken, true)).build());
        ReadOrderGroup orderGroup = command.startOrderGroup();
        UnfilteredPartitionIterator partitionIterator = command.queryStorageInternal(cfs, orderGroup);
        RowIteratorSanityCheck check = new RowIteratorSanityCheck(fromToken);
        long count = 0;
        while (partitionIterator.hasNext())
        {
            UnfilteredRowIterator cassandraRowIterator = partitionIterator.next();

            UnfilteredRowIterator rocksdbRowIterator = cfs.engine.queryStorage(
               cfs,
               SinglePartitionReadCommand.fullPartitionRead(cfs.metadata, 0, // Set time to be zero so we don't trim expring data.
                                                            cassandraRowIterator.partitionKey()));
            check.compare(cassandraRowIterator, rocksdbRowIterator);
            if (count++ % 1000 == 0)
                LOGGER.info(check.toString());
            if (limit > 0 && count >= limit)
                break;
        }
        return check.getReport();
    }

    private static final class InternalPartitionRangeReadCommand extends PartitionRangeReadCommand
    {
        private InternalPartitionRangeReadCommand(PartitionRangeReadCommand original)
        {
            super(original.isDigestQuery(),
                  original.digestVersion(),
                  original.isForThrift(),
                  original.metadata(),
                  original.nowInSec(),
                  original.columnFilter(),
                  original.rowFilter(),
                  original.limits(),
                  original.dataRange(),
                  Optional.empty());
        }

        private UnfilteredPartitionIterator queryStorageInternal(ColumnFamilyStore cfs,
                                                                 ReadOrderGroup orderGroup)
        {
            return queryStorage(cfs, orderGroup);
        }
    }
}
