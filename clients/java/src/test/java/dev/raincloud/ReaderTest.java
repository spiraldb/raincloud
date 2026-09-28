// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt8Vector;
import static org.junit.jupiter.api.Assertions.*;

/** Reads the shared fixture written by tests/reader_fixture.py. */
class ReaderTest {
    static final List<String> FORMATS = List.of("arrow", "parquet", "vortex");
    static final int ROWS = 8;
    /** Row 7's uint64 id is 2**64-1; every other id equals its row number. */
    static final int MAX_ID_ROW = 7;
    static final String MAX_ID = "18446744073709551615";
    /** Row 1 holds a null text and the double -0.0, whose raw bits are Long.MIN_VALUE. */
    static final int NULL_AND_NEGATIVE_ZERO_ROW = 1;
    static final long NEGATIVE_ZERO_BITS = Double.doubleToRawLongBits(-0.0);
    static final int ACCENTED_ROW = 2;

    Map<String,Object> options() throws Exception {
        String fixture=System.getProperty("raincloud.fixture");
        assertFalse(fixture.isEmpty(), "pass -Draincloud.fixture=path to shared fixture");
        return Raincloud.JSON.readValue(Files.readString(Path.of(fixture,"options.json")), new TypeReference<Map<String,Object>>() {});
    }
    @Test void streamAllFormatsAndReleaseEarly() throws Exception {
        for(String format:FORMATS) {
            try(var allocator=new RootAllocator()) {
                var ds=Raincloud.load("tiny",format,options());
                try(var reader=ds.batches(allocator,2)) {
                    assertEquals("reader-fixture",ds.metadata().get("catalog_id"));
                    assertEquals(format,ds.format());
                    assertEquals(4,ds.schema(allocator).getFields().size());
                    ds.close(); // closed early on purpose: the stream owns its source
                    int rows=0;
                    while(reader.loadNextBatch()) {
                        var root=reader.getVectorSchemaRoot();
                        assertTrue(root.getRowCount()<=2);
                        assertEquals("id",root.getSchema().getFields().get(0).getName());
                        for (int i=0; i<root.getRowCount(); i++) {
                            int row=rows+i;
                            assertEquals(row==MAX_ID_ROW ? MAX_ID : Integer.toString(row), Long.toUnsignedString(((UInt8Vector)root.getVector("id")).get(i)));
                            if(row==NULL_AND_NEGATIVE_ZERO_ROW) {
                                assertNull(root.getVector("text").getObject(i));
                                assertEquals(NEGATIVE_ZERO_BITS, Double.doubleToRawLongBits((Double)root.getVector("value").getObject(i)));
                            }
                            if(row==ACCENTED_ROW) assertEquals("é",root.getVector("text").getObject(i).toString());
                        }
                        rows+=root.getRowCount();
                    }
                    assertEquals(ROWS,rows);
                } finally { ds.close(); }
                try(var early=Raincloud.load("tiny",format,options());var reader=early.batches(allocator,1)) {
                    assertTrue(reader.loadNextBatch());
                    assertEquals(1,reader.getVectorSchemaRoot().getRowCount());
                }
            }
        }
    }
    @Test void writerQualifiedFormatAndTypedOfflineMiss() throws Exception {
        var opts=options();
        // One file per format: a writer is provenance, never something to ask for.
        assertEquals(RaincloudException.Kind.FORMAT_UNAVAILABLE,
            assertThrows(RaincloudException.class,()->Raincloud.load("tiny","parquet@rs",opts)).kind());
        opts.put("data_dir",Path.of(System.getProperty("raincloud.fixture"),"missing").toString());
        try(var ds=Raincloud.load("tiny","arrow",opts)) {
            assertEquals(ROWS,ds.metadata().get("rows"));
            assertEquals(RaincloudException.Kind.OFFLINE_MISS,assertThrows(RaincloudException.class,ds::path).kind());
        }
    }
    /** Same-size damage passes the catalog's size check and must fail to decode as CORRUPT_ARTIFACT. */
    @Test void damagedBytesAreCorruptArtifacts(@TempDir Path temp) throws Exception {
        var opts=options();
        Path source=Path.of((String)opts.get("data_dir"));
        try(var files=Files.walk(source)) {
            for(Path file:(Iterable<Path>)files::iterator) {
                Path copy=temp.resolve(source.relativize(file).toString());
                if(Files.isDirectory(file)) Files.createDirectories(copy);
                else Files.write(copy, new byte[(int)Files.size(file)]);
            }
        }
        opts.put("data_dir",temp.toString());
        for(String format:FORMATS) {
            try(var allocator=new RootAllocator(); var ds=Raincloud.load("tiny",format,opts)) {
                var error=assertThrows(RaincloudException.class,()->ds.batches(allocator,2));
                assertEquals(RaincloudException.Kind.CORRUPT_ARTIFACT,error.kind(),format+": "+error.getMessage());
            }
        }
    }
    /** Calls on one handle run concurrently, and close() waits for the ones in flight. */
    @Test void closeWaitsForConcurrentCalls(@TempDir Path temp) throws Exception {
        // A CLI that announces each run, then takes its time.
        Path started=temp.resolve("started"), slow=temp.resolve("slow-raincloud");
        String cli=Optional.ofNullable(System.getenv("RAINCLOUD_CLI")).orElse("raincloud");
        Files.writeString(slow,"#!/bin/sh\necho run >> '"+started+"'\nsleep 3\nexec '"+cli+"' \"$@\"\n");
        assertTrue(slow.toFile().setExecutable(true));
        var opts=options(); opts.put("cli",slow.toString());
        var ds=Raincloud.load("tiny","parquet",opts); // the first run
        var pool=Executors.newFixedThreadPool(2);
        try(var allocator=new RootAllocator()) {
            var batchesReturned=new AtomicBoolean();
            Future<Path> path=pool.submit(ds::path);
            Future<Integer> rows=pool.submit(()->{
                try(var reader=ds.batches(allocator,3)) {
                    batchesReturned.set(true);
                    int n=0;
                    while(reader.loadNextBatch()) n+=reader.getVectorSchemaRoot().getRowCount();
                    return n;
                }
            });
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);
            while(!Files.exists(started)||Files.readAllLines(started).size()<3) {
                assertTrue(System.nanoTime()<deadline,"the two calls never both started");
                Thread.sleep(20);
            }
            // Both CLI runs started before either call returned: they overlap.
            assertFalse(path.isDone()||batchesReturned.get(),"the calls ran one after the other");
            long before=System.nanoTime();
            ds.close();
            long waited=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-before);
            assertTrue(waited>=1000,"close() returned after "+waited+" ms, while both calls were in flight");
            assertTrue(path.get(60,TimeUnit.SECONDS).toString().endsWith(".parquet"));
            assertEquals(ROWS,rows.get(60,TimeUnit.SECONDS));
        } finally { pool.shutdownNow(); }
        assertThrows(IllegalStateException.class,ds::path);
        assertThrows(IllegalStateException.class,ds::metadata);
        try(var allocator=new RootAllocator()) {
            assertThrows(IllegalStateException.class,()->ds.batches(allocator,2));
        }
    }
    @Test void kindsCarryTheirAbiCodes() {
        for(var kind:RaincloudException.Kind.values()) assertEquals(kind,RaincloudException.Kind.of(kind.code));
        assertEquals(RaincloudException.Kind.INTERNAL,RaincloudException.Kind.of(99));
        assertEquals(14,RaincloudException.Kind.CORRUPT_ARTIFACT.code);
    }
}
