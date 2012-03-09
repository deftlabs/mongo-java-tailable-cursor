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

// Java
import java.util.EventListener;

/**
 * The tailable cursor error listener interface. This can be passed to the
 * options object if you want to handle errors.
 */
public interface TailableCursorErrorListener extends EventListener {

    /**
     * Called when an error is received when trying to pull docs from the
     * tailable cursor.
     */
    public void onError(final Throwable pT);
}

