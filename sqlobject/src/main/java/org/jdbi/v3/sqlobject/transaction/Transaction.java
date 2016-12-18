/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jdbi.v3.sqlobject.transaction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.transaction.TransactionCallback;
import org.jdbi.v3.core.transaction.TransactionException;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jdbi.v3.sqlobject.Handler;
import org.jdbi.v3.sqlobject.HandlerDecorator;
import org.jdbi.v3.sqlobject.SqlMethodDecoratingAnnotation;

/**
 * Causes the annotated method to be run in a transaction.
 * <p>
 * Nested <code>@Transaction</code> annotations (e.g. one method calls another method, where both methods have this
 * annotation) are collapsed into a single transaction. If the outer method annotation specifies an isolation level,
 * then the inner method must either specify the same level, or not specify any level.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@SqlMethodDecoratingAnnotation(Transaction.Decorator.class)
public @interface Transaction {
    TransactionIsolationLevel value() default TransactionIsolationLevel.INVALID_LEVEL;

    class Decorator implements HandlerDecorator {
        @Override
        public Handler decorateHandler(Handler base, Class<?> sqlObjectType, Method method) {
            final TransactionIsolationLevel isolation = method.getAnnotation(Transaction.class).value();

            return (target, args, handle) -> {
                Handle h = handle.getHandle();

                if (h.isInTransaction()) {
                    TransactionIsolationLevel currentLevel = h.getTransactionIsolationLevel();
                    if (currentLevel == isolation || isolation == TransactionIsolationLevel.INVALID_LEVEL) {
                        // Already in transaction. The outermost @Transaction method determines the transaction isolation level.
                        return base.invoke(target, args, handle);
                    } else {
                        throw new TransactionException("Tried to execute nested @Transaction(" + isolation + "), " +
                                "but already running in a transaction with isolation level " + currentLevel + ".");
                    }
                }

                TransactionCallback<Object, Exception> callback = th -> base.invoke(target, args, handle);

                if (isolation == TransactionIsolationLevel.INVALID_LEVEL) {
                    return h.inTransaction(callback);
                } else {
                    return h.inTransaction(isolation, callback);
                }
            };
        }
    }
}
