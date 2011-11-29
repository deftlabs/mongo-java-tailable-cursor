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

/**
 * The tailable cursor exception.
 */
public class TailableCursorException extends RuntimeException {



    public TailableCursorException() { super(); }

    public TailableCursorException(final String pMsg) { super(pMsg); }

    public TailableCursorException(final String pMsg, final String pErrorCode)
    { super(pMsg); _errorCode = pErrorCode; }

    public TailableCursorException(final String pMsg, final Throwable pT) { super(pMsg, pT); }

    public TailableCursorException(final Throwable pT) { super(pT); }

    public String getErrorCode() { return _errorCode; }

    public boolean hasErrorCode() { return _errorCode != null; }

    private String _errorCode = null;

    private static final long serialVersionUID = -4415279469780082174L;

    public static final String NON_CAPPED_COLLECTION = "1001";
    public static final String NO_COLLECTION_FOUND = "1002";

}

