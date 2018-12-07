/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import java.util.function.Predicate;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class PredicateMatcher<T> extends TypeSafeMatcher<T> {

    private final Predicate<T> predicate;
    
    private PredicateMatcher(Predicate<T> predicate) {
        this.predicate = predicate;
    }
    
    public static <T> PredicateMatcher<T> matchesPredicate(Predicate<T> predicate) {
        return new PredicateMatcher<>(predicate);
    }

    @Override
    protected boolean matchesSafely(T item) {
        return predicate.test(item);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches predicate ").appendValue(predicate);
    }

    @Override
    protected void describeMismatchSafely(T item, Description mismatchDescription) {
        mismatchDescription.appendText("failed to match predicate ").appendValue(item);
    }  
}
