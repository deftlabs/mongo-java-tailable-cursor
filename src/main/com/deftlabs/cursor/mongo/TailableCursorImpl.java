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

// Lib
import com.deftlabs.cursor.mongo.TailableCursor;
import com.deftlabs.cursor.mongo.TailableCursorOptions;

// Mongo
import com.mongodb.Bytes;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

// Java
import java.util.Queue;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * The tailable cursor interface.
 */
public class TailableCursorImpl extends TailableCursor {

    /**
     * Returns the next object in the cursor. This call blocks until an object
     * is available.
     * @throws InterruptedException
     */
    @Override
    public DBObject nextDoc() throws InterruptedException {
        if (_options.hasDocListener())
        { throw new TailableCursorException("Can't use doc listener and nextDoc together"); }

        DBObject doc = null;

        while (true) {
            doc = _docQueue.poll();
            if (doc != null) return doc;
            lock();
        }
    }

    private void lock() throws InterruptedException {
        boolean wasInterrupted = false;

        final Thread current = Thread.currentThread();

        _waiters.add(current);

        while (_waiters.peek() != current || !_locked.compareAndSet(false, true)) {
            LockSupport.park();

            // If interrupted, get out of here
            if (Thread.interrupted()) { wasInterrupted = true; break; }
        }

        _waiters.remove();

        if (wasInterrupted) { current.interrupt(); throw new InterruptedException(); }
    }

    private void unlock() {
        _locked.set(false);
        LockSupport.unpark(_waiters.peek());
    }

    /**
     * Called to start the tailable cursor.
     */
    @Override
    public synchronized void start() {
        if (_running.get()) throw new TailableCursorException("Already running");
        _running.set(true);
        _cursorReader.start();
    }

    /**
     * Called to stop the tailable cursor. This causes an Interrupted exception to
     * be thrown in the nextDoc method.
     */
    @Override
    public void stop() {
        if (!_running.get()) throw new TailableCursorException("Not running");
        _running.set(false);
        if (_cursorReader != null) _cursorReader.interrupt();
        for (final Thread t : _waiters) t.interrupt();
    }

    /**
     * A Thread that pulls the data off the cursor.
     */
    private class CursorReader extends Thread {
        @Override
        public void run() {
            while (_running.get()) {
                try {
                    _mongo.getDB(_options.getDatabaseName()).requestStart();
                    final DBCursor cur = createCursor();
                    try {
                        while (cur.hasNext() && _running.get()) {
                            final DBObject doc = cur.next();

                            if (doc == null) break;

                            if (_options.hasDocListener()) { _options.getDocListener().nextDoc(doc);
                            } else { _docQueue.put(doc); unlock(); }
                        }
                    } finally {
                        try { if (cur != null) cur.close(); } catch (final Throwable t) { /* nada */ }
                        try { _mongo.getDB(_options.getDatabaseName()).requestDone(); } catch (final Throwable t) { /* nada */ }
                    }

                    if (_options.getNoDocSleepTime() > 0) Thread.sleep(_options.getNoDocSleepTime());

                } catch (final InterruptedException ie) { break;
                } catch (final Throwable t) { if (handleException(t)) break; }
            }
        }

        private DBCursor createCursor() {
            final DBCollection col = _mongo.getDB(_options.getDatabaseName()).getCollection(_options.getCollectionName());

            return col.find(_options.getInitialQuery()).sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA);
        }

        private boolean handleException(final Throwable pT) {
            if (_options.hasErrorListener()) {
                try {
                    // Call the error listener.
                    _options.getErrorListener().onError(pT);
                } catch (final Throwable t) { _logger.log(Level.SEVERE, pT.getMessage(), pT); }
            } else { _logger.log(Level.SEVERE, pT.getMessage(), pT); }

            if (_options.getErrorSleepTime() <= 0) return false;

            try { Thread.sleep(_options.getErrorSleepTime());
            } catch (final InterruptedException ie) { return true; }

            return false;
        }
    }

    @Override
    public boolean isRunning() { return _running.get(); }

    /**
     * Construct a new object.
     * @param pOptions The cursor options.
     */
    public TailableCursorImpl(final TailableCursorOptions pOptions) {

        _options = pOptions;

        try {
            _mongo = new Mongo(new MongoURI(_options.getMongoUri()));

            if (!_mongo.getDB(_options.getDatabaseName()).collectionExists(_options.getCollectionName())) {
                if (_options.getAssertIfNoCappedCollection()) {
                    throw new TailableCursorException(  "Not a capped collection - db: "
                                                        + _options.getDatabaseName()
                                                        + " - collection: "
                                                        + _options.getCollectionName());
                }



                final BasicDBObject options = new BasicDBObject("capped", true);
                options.put("size", _options.getDefaultCappedCollectionSize());
                _mongo.getDB(_options.getDatabaseName()).createCollection(_options.getCollectionName(), options);
            }

            // TODO: Verify the collection is capped

            _cursorReader = new CursorReader();

        } catch (final UnknownHostException uhe) {
            throw new TailableCursorException("Host not found - uri: " + _options.getMongoUri(), uhe);
        } catch (final Throwable t) { throw new TailableCursorException(t); }
    }

    private final Mongo _mongo;
    private final AtomicBoolean _running = new AtomicBoolean(false);
    private final AtomicBoolean _locked = new AtomicBoolean(false);
    private final TailableCursorOptions _options;
    private final Queue<Thread> _waiters = new ConcurrentLinkedQueue<Thread>();

    private final Logger _logger = Logger.getLogger("com.deftlabs.cursor.mongo.TailableCursor");

    private final LinkedBlockingQueue<DBObject> _docQueue = new LinkedBlockingQueue<DBObject>(1);
    private final CursorReader _cursorReader;
}

