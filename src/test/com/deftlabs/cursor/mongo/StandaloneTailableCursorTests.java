/**
 * Copyright 2011, Deft Labs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.deftlabs.cursor.mongo;

// Mongo
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

// JUnit
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

// Java
import java.util.Date;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The standalone threaded tailable cursor tests.
 */
public final class StandaloneTailableCursorTests {

    private void test() throws Exception {

        getDb().dropDatabase();

        final TailableCursorOptions options
        = new TailableCursorOptions("mongodb://127.0.0.1:27017", "com_deftlabs_cursor_mongo_tailableCursorTest", "test");

        final TailableCursor tailableCursor = new TailableCursorImpl(options);

        try {
            tailableCursor.start();

            for (int idx=0; idx < THREAD_COUNT; idx++) new Thread(new ReadTest(tailableCursor)).start();

            final CountDownLatch writerCountDownLatch = new CountDownLatch(1);

            new Thread(new Writer(writerCountDownLatch)).start();

            writerCountDownLatch.await();

            if (_readCount.get() != WRITE_COUNT)
            { throw new IllegalStateException("Counts off - expected: " + WRITE_COUNT + " - actual: " + _readCount.get()); }

            //System.out.println("----- read count: " + _readCount.get());

        } finally {
            tailableCursor.stop();
            getDb().dropDatabase();
        }
    }

    private class ReadTest implements Runnable {

        private ReadTest(final TailableCursor pTailableCursor)
        { _tailableCursor = pTailableCursor; }

        @Override
        public void run() {
            while (true) {
                try {
                    final BasicDBObject doc = (BasicDBObject)_tailableCursor.nextDoc();

                    if (doc == null) System.out.println("------ yikes - doc is null");

                    final int id = doc.getInt("_id");

                    if (_seenIds.contains(id)) System.out.println("---- doh - contains a duplicate: " + id);

                    _seenIds.add(id);

                    _readCount.incrementAndGet();

                } catch (final InterruptedException ie) {
                    //System.out.println("--------- ReadTest interrupted");
                    break;
                }
            }
        }

        private final TailableCursor _tailableCursor;
    }

    private class Writer implements Runnable {

        private Writer(final CountDownLatch pWriterCountDownLatch) {
            _writerCountDownLatch = pWriterCountDownLatch;
        }

       @Override
        public void run() {
            for (int idx=0; idx < WRITE_COUNT; idx++) {
                final BasicDBObject doc = new BasicDBObject("_id", idx);
                doc.put("ts", new Date());
                getCollection().insert(doc);
            }

            try { Thread.sleep(2000); } catch (final InterruptedException ie) { throw new IllegalStateException(ie); }

            _writerCountDownLatch.countDown();
        }
        private final CountDownLatch _writerCountDownLatch;
    }

    private DB getDb()
    { return _mongo.getDB("com_deftlabs_cursor_mongo_tailableCursorTest"); }

    private DBCollection getCollection()
    { return _mongo.getDB("com_deftlabs_cursor_mongo_tailableCursorTest").getCollection("test"); }

    private StandaloneTailableCursorTests() throws Exception
    { _mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017")); }

    private static final int THREAD_COUNT = 10;

    private static final int WRITE_COUNT = 100000;

    private final AtomicLong _readCount = new AtomicLong(0);

    private final Set<Integer> _seenIds = new HashSet<Integer>();

    public static void main(final String [] pArgs) throws Exception {
        final StandaloneTailableCursorTests tests = new StandaloneTailableCursorTests();
        tests.test();

    }

    private final Mongo _mongo;
}

