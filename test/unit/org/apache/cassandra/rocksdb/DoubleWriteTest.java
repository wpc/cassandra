package org.apache.cassandra.rocksdb;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.SinglePartitionReadCommand;

import static org.junit.Assert.assertEquals;

public class DoubleWriteTest extends RocksDBTestBase
{
    @BeforeClass
    public static void classSetUp() throws Exception
    {
        RocksDBTestBase.classSetUp();
        System.setProperty("cassandra.rocksdb.double_write", "true");
    }

    @AfterClass
    public static void classTeardown() throws Exception
    {
        System.clearProperty("cassandra.rocksdb.double_write");
        RocksDBTestBase.classTeardown();
    }

    @Test
    public void testDoubleWrite() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, v text, PRIMARY KEY (p))");
        execute("INSERT INTO %s(p, v) values (?, ?)", "p1", "v0");

        SinglePartitionReadCommand readCommand = readCommand("p1", "v");

        assertEquals(1, queryEngine(readCommand).size());
        assertEquals(1, queryCassandraStorage(readCommand).size());
    }
}
