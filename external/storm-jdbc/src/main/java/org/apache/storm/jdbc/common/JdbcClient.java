/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.jdbc.common;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JdbcClient {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcClient.class);

	protected ConnectionProvider connectionProvider;
	protected int queryTimeoutSecs;

	public JdbcClient(ConnectionProvider connectionProvider, int queryTimeoutSecs) {
		this.connectionProvider = connectionProvider;
		this.queryTimeoutSecs = queryTimeoutSecs;
	}

	public void insert(String tableName, List<List<Column>> columnLists) {
		String query = constructInsertQuery(tableName, columnLists);
		executeInsertQuery(query, columnLists);
	}

	public void executeInsertQuery(String query, List<List<Column>> columnLists) {
		Connection connection = null;
		try {
			connection = connectionProvider.getConnection();
			boolean autoCommit = connection.getAutoCommit();
			if (autoCommit) {
				connection.setAutoCommit(false);
			}

			LOG.debug("Executing query {}", query);

			PreparedStatement preparedStatement = connection.prepareStatement(query);
			if (queryTimeoutSecs > 0) {
				preparedStatement.setQueryTimeout(queryTimeoutSecs);
			}

			for (List<Column> columnList : columnLists) {
				setPreparedStatementParams(preparedStatement, columnList);
				preparedStatement.addBatch();
			}

			int[] results = preparedStatement.executeBatch();
			if (Arrays.asList(results).contains(Statement.EXECUTE_FAILED)) {
				connection.rollback();
				throw new RuntimeException("failed at least one sql statement in the batch, operation rolled back.");
			} else {
				try {
					connection.commit();
				} catch (SQLException e) {
					throw new RuntimeException("Failed to commit insert query " + query, e);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Failed to execute insert query " + query, e);
		} finally {
			closeConnection(connection);
		}
	}

	private String constructInsertQuery(String tableName, List<List<Column>> columnLists) {
		StringBuilder sb = new StringBuilder();
		sb.append("Insert into ").append(tableName).append(" (");
		Collection<String> columnNames = Collections2.transform(columnLists.get(0), new Function<Column, String>() {
			@Override
			public String apply(Column input) {
				return input.getColumnName();
			}
		});
		String columns = Joiner.on(",").join(columnNames);
		sb.append(columns).append(") values ( ");

		String placeHolders = StringUtils.chop(StringUtils.repeat("?,", columnNames.size()));
		sb.append(placeHolders).append(")");

		return sb.toString();
	}

	public List<List<Column>> select(String sqlQuery, List<Column> queryParams) {
		Connection connection = null;
		try {
			connection = connectionProvider.getConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
			if (queryTimeoutSecs > 0) {
				preparedStatement.setQueryTimeout(queryTimeoutSecs);
			}
			setPreparedStatementParams(preparedStatement, queryParams);
			ResultSet resultSet = preparedStatement.executeQuery();
			List<List<Column>> rows = Lists.newArrayList();
			while (resultSet.next()) {
				ResultSetMetaData metaData = resultSet.getMetaData();
				int columnCount = metaData.getColumnCount();
				List<Column> row = Lists.newArrayList();
				for (int i = 1; i <= columnCount; i++) {
					String columnLabel = metaData.getColumnLabel(i);
					int columnType = metaData.getColumnType(i);
					Class columnJavaType = Util.getJavaType(columnType);
					if (columnJavaType.equals(String.class)) {
						row.add(new Column<String>(columnLabel, resultSet.getString(columnLabel), columnType));
					} else if (columnJavaType.equals(Byte.class)) {
						Byte val = resultSet.getByte(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.add(new Column<Byte>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Integer.class)) {
						Integer val = resultSet.getInt(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.add(new Column<Integer>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Double.class)) {
						Double val = resultSet.getDouble(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.add(new Column<Double>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Float.class)) {
						Float val = resultSet.getFloat(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.add(new Column<Float>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Short.class)) {
						Short val = resultSet.getShort(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.add(new Column<Short>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Boolean.class)) {
						Boolean val = resultSet.getBoolean(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.add(new Column<Boolean>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(byte[].class)) {
						row.add(new Column<byte[]>(columnLabel, resultSet.getBytes(columnLabel), columnType));
					} else if (columnJavaType.equals(Long.class)) {
						Long val = resultSet.getLong(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.add(new Column<Long>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Date.class)) {
						row.add(new Column<Date>(columnLabel, resultSet.getDate(columnLabel), columnType));
					} else if (columnJavaType.equals(Time.class)) {
						row.add(new Column<Time>(columnLabel, resultSet.getTime(columnLabel), columnType));
					} else if (columnJavaType.equals(Timestamp.class)) {
						row.add(new Column<Timestamp>(columnLabel, resultSet.getTimestamp(columnLabel), columnType));
					} else {
						throw new RuntimeException("type =  " + columnType + " for column " + columnLabel + " not supported.");
					}
				}
				rows.add(row);
			}
			return rows;
		} catch (SQLException e) {
			throw new RuntimeException("Failed to execute select query " + sqlQuery, e);
		} finally {
			closeConnection(connection);
		}
	}

	public List<Map<String, Column>> select2MapRow(String sqlQuery, List<Column> queryParams) {
		Connection connection = null;
		try {
			connection = connectionProvider.getConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
			if (queryTimeoutSecs > 0) {
				preparedStatement.setQueryTimeout(queryTimeoutSecs);
			}
			setPreparedStatementParams(preparedStatement, queryParams);
			ResultSet resultSet = preparedStatement.executeQuery();
			List<Map<String, Column>> rows = Lists.newArrayList();
			while (resultSet.next()) {
				ResultSetMetaData metaData = resultSet.getMetaData();
				int columnCount = metaData.getColumnCount();
				Map<String, Column> row = Maps.newHashMap();
				for (int i = 1; i <= columnCount; i++) {
					String columnLabel = metaData.getColumnLabel(i);
					int columnType = metaData.getColumnType(i);
					Class columnJavaType = Util.getJavaType(columnType);
					if (columnJavaType.equals(String.class)) {
						row.put(columnLabel, new Column<String>(columnLabel, resultSet.getString(columnLabel), columnType));
					} else if (columnJavaType.equals(Byte.class)) {
						Byte val = resultSet.getByte(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.put(columnLabel, new Column<Byte>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Integer.class)) {
						Integer val = resultSet.getInt(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.put(columnLabel, new Column<Integer>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Double.class)) {
						Double val = resultSet.getDouble(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.put(columnLabel, new Column<Double>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Float.class)) {
						Float val = resultSet.getFloat(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.put(columnLabel, new Column<Float>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Short.class)) {
						Short val = resultSet.getShort(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.put(columnLabel, new Column<Short>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Boolean.class)) {
						Boolean val = resultSet.getBoolean(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.put(columnLabel, new Column<Boolean>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(byte[].class)) {
						row.put(columnLabel, new Column<byte[]>(columnLabel, resultSet.getBytes(columnLabel), columnType));
					} else if (columnJavaType.equals(Long.class)) {
						Long val = resultSet.getLong(columnLabel);
						if (resultSet.wasNull()) {
							val = null;
						}
						row.put(columnLabel, new Column<Long>(columnLabel, val, columnType));
					} else if (columnJavaType.equals(Date.class)) {
						row.put(columnLabel, new Column<Date>(columnLabel, resultSet.getDate(columnLabel), columnType));
					} else if (columnJavaType.equals(Time.class)) {
						row.put(columnLabel, new Column<Time>(columnLabel, resultSet.getTime(columnLabel), columnType));
					} else if (columnJavaType.equals(Timestamp.class)) {
						row.put(columnLabel, new Column<Timestamp>(columnLabel, resultSet.getTimestamp(columnLabel), columnType));
					} else {
						throw new RuntimeException("type =  " + columnType + " for column " + columnLabel + " not supported.");
					}
				}
				rows.add(row);
			}
			return rows;
		} catch (SQLException e) {
			throw new RuntimeException("Failed to execute select query " + sqlQuery, e);
		} finally {
			closeConnection(connection);
		}
	}

	public List<Column> getColumnSchema(String tableName) {
		Connection connection = null;
		List<Column> columns = new ArrayList<Column>();
		try {
			connection = connectionProvider.getConnection();
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet resultSet = metaData.getColumns(null, null, tableName, null);
			while (resultSet.next()) {
				columns.add(new Column(resultSet.getString("COLUMN_NAME"), resultSet.getInt("DATA_TYPE")));
			}
			return columns;
		} catch (SQLException e) {
			throw new RuntimeException("Failed to get schema for table " + tableName, e);
		} finally {
			closeConnection(connection);
		}
	}

	@SuppressWarnings("rawtypes")
	public Map<String, Class> getColumnSchemaToMap(String tableName) {
		Connection connection = null;
		Map<String, Class> columns = new HashMap<String, Class>();
		try {
			connection = connectionProvider.getConnection();
			DatabaseMetaData metaData = connection.getMetaData();
			ResultSet resultSet = metaData.getColumns(null, null, tableName, null);
			while (resultSet.next()) {
				String columnName = resultSet.getString("COLUMN_NAME");
				int columnType = resultSet.getInt("DATA_TYPE");
				Class columnJavaType = Util.getJavaType(columnType);
				columns.put(columnName, columnJavaType);
			}
			return columns;
		} catch (SQLException e) {
			throw new RuntimeException("Failed to get schema for table " + tableName, e);
		} finally {
			closeConnection(connection);
		}
	}

	public void executeSql(String sql) {
		Connection connection = null;
		try {
			connection = connectionProvider.getConnection();
			Statement statement = connection.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException("Failed to execute SQL", e);
		} finally {
			closeConnection(connection);
		}
	}

	public void executeBatchSql(List<String> sqlList) {
		Connection connection = null;
		try {
			connection = connectionProvider.getConnection();
			Statement statement = connection.createStatement();
			connection.setAutoCommit(false);
			for (String sql : sqlList) {
				statement.addBatch(sql);
			}
			int[] results = statement.executeBatch();
			if (Arrays.asList(results).contains(Statement.EXECUTE_FAILED)) {
				connection.rollback();
				throw new RuntimeException("failed at least one sql statement in the batch, operation rolled back.");
			} else {
				try {
					connection.commit();
				} catch (SQLException e) {
					throw new RuntimeException("Failed to commit insert query ", e);
				}
			}
		} catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				throw new RuntimeException("Failed to rollback jdbc connection ", e);
			}
			throw new RuntimeException("Failed to execute SQL", e);
		} finally {
			closeConnection(connection);
		}
	}

	protected void setPreparedStatementParams(PreparedStatement preparedStatement, List<Column> columnList) throws SQLException {
		int index = 1;
		for (Column column : columnList) {
			Class columnJavaType = Util.getJavaType(column.getSqlType());
			if (column.getVal() == null) {
				preparedStatement.setNull(index, column.getSqlType());
			} else if (columnJavaType.equals(String.class)) {
				preparedStatement.setString(index, (String) column.getVal());
			} else if (columnJavaType.equals(Integer.class)) {
				preparedStatement.setInt(index, (Integer) column.getVal());
			} else if (columnJavaType.equals(Double.class)) {
				preparedStatement.setDouble(index, (Double) column.getVal());
			} else if (columnJavaType.equals(Float.class)) {
				preparedStatement.setFloat(index, (Float) column.getVal());
			} else if (columnJavaType.equals(Short.class)) {
				preparedStatement.setShort(index, (Short) column.getVal());
			} else if (columnJavaType.equals(Boolean.class)) {
				preparedStatement.setBoolean(index, (Boolean) column.getVal());
			} else if (columnJavaType.equals(byte[].class)) {
				preparedStatement.setBytes(index, (byte[]) column.getVal());
			} else if (columnJavaType.equals(Long.class)) {
				preparedStatement.setLong(index, (Long) column.getVal());
			} else if (columnJavaType.equals(Date.class)) {
				preparedStatement.setDate(index, (Date) column.getVal());
			} else if (columnJavaType.equals(Time.class)) {
				preparedStatement.setTime(index, (Time) column.getVal());
			} else if (columnJavaType.equals(Timestamp.class)) {
				preparedStatement.setTimestamp(index, (Timestamp) column.getVal());
			} else {
				throw new RuntimeException("Unknown type of value " + column.getVal() + " for column " + column.getColumnName());
			}
			++index;
		}
	}

	protected void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new RuntimeException("Failed to close connection", e);
			}
		}
	}

	public Map<String, Integer> fetchTableColumnMetas(String sql) {
		Connection connection = null;
		try {
			connection = connectionProvider.getConnection();
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			Map<String, Integer> row = Maps.newHashMap();
			for (int i = 1; i <= columnCount; i++) {
				String columnLabel = metaData.getColumnLabel(i);
				int columnType = metaData.getColumnType(i);
				Class columnJavaType = Util.getJavaType(columnType);
				row.put(columnLabel, columnType);
			}
			return row;
		} catch (SQLException e) {
			throw new RuntimeException("Failed to execute select query " + sql, e);
		} finally {
			closeConnection(connection);
		}
	}
}
