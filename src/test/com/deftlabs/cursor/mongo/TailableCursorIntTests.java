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
import com.deftlabs.cursor.mongo.TailableCursorImpl;

// Mongo
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.MongoURI;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

// JUnit
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

// Java
import java.util.concurrent.TimeUnit;

/**
 * Test the distributed lock. You must be running mongo on localhost:27017 for this
 * test to work.
 */
public final class TailableCursorIntTests {

    @Test
    public void testSimple() throws Exception {
        getDb().dropDatabase();

        final TailableCursorOptions options
        = new TailableCursorOptions("mongodb://127.0.0.1:27017", "com_deftlabs_cursor_mongo_tailableCursorTest", "test");
        options.setDefaultCappedCollectionSize(10485760);

        final TailableCursor tailableCursor = new TailableCursorImpl(options);

        for (int idx=0; idx < 20; idx++) getCollection().insert(new BasicDBObject());

        try {
            tailableCursor.start();

            for (int idx=0; idx < 20; idx++) assertNotNull(tailableCursor.nextDoc());

        } finally { tailableCursor.stop(); }
    }

    @Before
    public void init() throws Exception {
        _mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017"));
    }

    private DB getDb()
    { return _mongo.getDB("com_deftlabs_cursor_mongo_tailableCursorTest"); }

    private DBCollection getCollection()
    { return _mongo.getDB("com_deftlabs_cursor_mongo_tailableCursorTest").getCollection("test"); }

    private Mongo _mongo;
}

