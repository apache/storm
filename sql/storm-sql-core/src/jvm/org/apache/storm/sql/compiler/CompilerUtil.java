/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.sql.compiler;

import static org.apache.calcite.rel.RelFieldCollation.Direction;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection;
import static org.apache.calcite.sql.validate.SqlMonotonicity.INCREASING;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;

import org.apache.storm.sql.calcite.ParallelStreamableTable;
import org.apache.storm.sql.calcite.ParallelTable;
import org.apache.storm.sql.parser.ColumnConstraint;

public class CompilerUtil {

    public static class TableBuilderInfo {
        private final RelDataTypeFactory typeFactory;
        private final ArrayList<FieldType> fields = new ArrayList<>();
        private final ArrayList<Object[]> rows = new ArrayList<>();
        private int primaryKey = -1;
        private Integer parallelismHint;
        private SqlMonotonicity primaryKeyMonotonicity;
        private Statistic stats;

        public TableBuilderInfo(RelDataTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
        }

        public TableBuilderInfo field(String name, SqlTypeName type) {
            return field(name, typeFactory.createSqlType(type));
        }

        public TableBuilderInfo field(String name, RelDataType type) {
            fields.add(new FieldType(name, type));
            return this;
        }

        public TableBuilderInfo field(String name, SqlTypeName type, ColumnConstraint constraint) {
            interpretConstraint(constraint, fields.size());
            return field(name, typeFactory.createSqlType(type));
        }

        public TableBuilderInfo field(String name, RelDataType type, ColumnConstraint constraint) {
            interpretConstraint(constraint, fields.size());
            fields.add(new FieldType(name, type));
            return this;
        }

        public TableBuilderInfo field(String name, SqlDataTypeSpec type, ColumnConstraint constraint) {
            RelDataType dataType = type.deriveType(typeFactory);
            interpretConstraint(constraint, fields.size());
            fields.add(new FieldType(name, dataType));
            return this;
        }

        private void interpretConstraint(ColumnConstraint constraint, int fieldIdx) {
            if (constraint instanceof ColumnConstraint.PrimaryKey) {
                ColumnConstraint.PrimaryKey pk = (ColumnConstraint.PrimaryKey) constraint;
                Preconditions.checkState(primaryKey == -1, "There are more than one primary key in the table");
                primaryKey = fieldIdx;
                primaryKeyMonotonicity = pk.monotonicity();
            }
        }

        public TableBuilderInfo statistics(Statistic stats) {
            this.stats = stats;
            return this;
        }

        @VisibleForTesting
        public TableBuilderInfo rows(Object[] data) {
            rows.add(data);
            return this;
        }

        public TableBuilderInfo parallelismHint(int parallelismHint) {
            this.parallelismHint = parallelismHint;
            return this;
        }

        public StreamableTable build() {
            final Statistic stat = buildStatistic();

            final Table tbl = new ParallelTable() {
                @Override
                public Integer parallelismHint() {
                    return parallelismHint;
                }

                @Override
                public int primaryKey() {
                    return primaryKey;
                }

                @Override
                public RelDataType getRowType(
                        RelDataTypeFactory relDataTypeFactory) {
                    RelDataTypeFactory.FieldInfoBuilder b = relDataTypeFactory.builder();
                    for (FieldType f : fields) {
                        b.add(f.name, f.relDataType);
                    }
                    return b.build();
                }

                @Override
                public Statistic getStatistic() {
                    return stat != null ? stat : Statistics.of(rows.size(),
                            ImmutableList.<ImmutableBitSet>of());
                }

                @Override
                public Schema.TableType getJdbcTableType() {
                    return Schema.TableType.STREAM;
                }

                @Override
                public boolean isRolledUp(String column) {
                    return false;
                }

                @Override
                public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
                                                            CalciteConnectionConfig config) {
                    return false;
                }
            };

            return new ParallelStreamableTable() {
                @Override
                public int primaryKey() {
                    return primaryKey;
                }

                @Override
                public Integer parallelismHint() {
                    return parallelismHint;
                }

                @Override
                public Table stream() {
                    return tbl;
                }

                @Override
                public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
                    return tbl.getRowType(relDataTypeFactory);
                }

                @Override
                public Statistic getStatistic() {
                    return tbl.getStatistic();
                }

                @Override
                public Schema.TableType getJdbcTableType() {
                    return Schema.TableType.STREAM;
                }

                @Override
                public boolean isRolledUp(String column) {
                    return false;
                }

                @Override
                public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
                                                            CalciteConnectionConfig config) {
                    return false;
                }
            };
        }

        private Statistic buildStatistic() {
            if (stats != null || primaryKey == -1) {
                return stats;
            }
            Direction dir = primaryKeyMonotonicity == INCREASING ? ASCENDING : DESCENDING;
            RelFieldCollation collation = new RelFieldCollation(primaryKey, dir, NullDirection.UNSPECIFIED);
            return Statistics.of(fields.size(), ImmutableList.of(ImmutableBitSet.of(primaryKey)),
                                 ImmutableList.of(RelCollations.of(collation)));
        }

        private static class FieldType {
            private final String name;
            private final RelDataType relDataType;

            private FieldType(String name, RelDataType relDataType) {
                this.name = name;
                this.relDataType = relDataType;
            }

        }
    }
}
