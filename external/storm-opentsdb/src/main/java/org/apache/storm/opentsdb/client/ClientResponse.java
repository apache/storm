/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.opentsdb.client;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.opentsdb.OpenTsdbMetricDatapoint;

/**
 * This class represents the response from OpenTsdb for a request sent.
 */
public interface ClientResponse extends Serializable {


    class Summary implements ClientResponse {
        private int failed;
        private int success;
        private int timeouts;

        public Summary() {
        }

        public Summary(int success, int failed, int timeouts) {
            this.failed = failed;
            this.success = success;
            this.timeouts = timeouts;
        }

        public int getFailed() {
            return failed;
        }

        public int getSuccess() {
            return success;
        }

        public int getTimeouts() {
            return timeouts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Summary)) {
                return false;
            }

            Summary summary = (Summary) o;

            if (failed != summary.failed) {
                return false;
            }
            if (success != summary.success) {
                return false;
            }
            return timeouts == summary.timeouts;

        }

        @Override
        public int hashCode() {
            int result = failed;
            result = 31 * result + success;
            result = 31 * result + timeouts;
            return result;
        }

        @Override
        public String toString() {
            return "Summary{"
                    + "failed=" + failed
                    + ", success=" + success
                    + ", timeouts=" + timeouts
                    + '}';
        }
    }

    class Details extends Summary {
        private List<Error> errors;

        public Details() {
        }

        public Details(int success, int failed, int timeouts, List<Error> errors) {
            super(success, failed, timeouts);
            this.errors = errors;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Details)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }

            Details details = (Details) o;

            return errors.equals(details.errors);

        }

        public List<Error> getErrors() {
            return errors;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + errors.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Details{"
                    + "errors=" + errors
                    + super.toString()
                    + '}';
        }

        public static class Error implements Serializable {
            private String error;
            private OpenTsdbMetricDatapoint datapoint;

            public Error() {
            }

            public Error(String error, OpenTsdbMetricDatapoint datapoint) {
                this.error = error;
                this.datapoint = datapoint;
            }

            public String getError() {
                return error;
            }

            public OpenTsdbMetricDatapoint getDatapoint() {
                return datapoint;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof Error)) {
                    return false;
                }

                Error error1 = (Error) o;

                if (!error.equals(error1.error)) {
                    return false;
                }
                return datapoint.equals(error1.datapoint);

            }

            @Override
            public int hashCode() {
                int result = error.hashCode();
                result = 31 * result + datapoint.hashCode();
                return result;
            }

            @Override
            public String toString() {
                return "Error{"
                        + "error='" + error + '\''
                        + ", datapoint=" + datapoint
                        + '}';
            }
        }
    }
}
