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
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

/**
 * The tailable cursor options object.
 */
public class TailableCursorOptions {

    public void setErrorListener(final TailableCursorErrorListener pV) { _errorListener = pV; }
    public TailableCursorErrorListener getErrorListener() { return _errorListener; }
    public boolean hasErrorListener() { return _errorListener != null; }

    public void setInitialQuery(final DBObject pV) { _initialQuery = pV; }
    public DBObject getInitialQuery() { return _initialQuery; }

    public void setNoDocSleepTime(final long pV) { _noDocSleepTime = pV; }
    public long getNoDocSleepTime() { return _noDocSleepTime; }

    public void setErrorSleepTime(final long pV) { _errorSleepTime = pV; }
    public long getErrorSleepTime() { return _errorSleepTime; }

    public String getMongoUri() { return _mongoUri; }

    public void setAssertIfNoCappedCollection(final boolean pV) { _assertIfNoCappedCollection = pV; }
    public boolean getAssertIfNoCappedCollection() { return _assertIfNoCappedCollection; }

    public void setDefaultCappedCollectionSize(final long pV) { _defaultCappedCollectionSize = pV; }

    /**
     * If the capped collection does not exist and assertIfNoCappedCollection == false,
     * then a new capped collection will be created with this size. The defalt behavior
     * of this library is to create a capped collection if one does not exist.
     */
    public long getDefaultCappedCollectionSize() { return _defaultCappedCollectionSize; }

    public String getDatabaseName() { return _databaseName; }
    public String getCollectionName() { return _collectionName; }

    private TailableCursorErrorListener _errorListener;
    private DBObject _initialQuery = new BasicDBObject();
    private long _noDocSleepTime = 100; // time in ms
    private long _errorSleepTime = 1000; // time in ms

    private long _defaultCappedCollectionSize = 209715200l; // size in bytes

    private boolean _assertIfNoCappedCollection = false;

    private final String _mongoUri;
    private final String _databaseName;
    private final String _collectionName;

    /**
     * The only required params are the uri, database and collection names.
     * @param pMongoUri The uri to connect to the server.
     * @param pDatabaseName The database name.
     * @param pCollectionName The collection name. If this is not a capped collection, an
     * exception will be thrown.
     */
    public TailableCursorOptions(   final String pMongoUri,
                                    final String pDatabaseName,
                                    final String pCollectionName)
    {
        _mongoUri = pMongoUri;
        _databaseName = pDatabaseName;
        _collectionName = pCollectionName;
    }
}

