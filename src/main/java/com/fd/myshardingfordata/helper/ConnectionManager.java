package com.fd.myshardingfordata.helper;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fd.myshardingfordata.annotation.ColumnRule;
import com.fd.myshardingfordata.annotation.MyIndex;

/**
 * 数据库连接管理
 * 
 * @author 符冬
 *
 */
public final class ConnectionManager implements IConnectionManager {
	@Override
	public Connection getConnection() {
		return getWriteConnection();
	}

	@Override
	public Connection getConnection(boolean readOnly) {
		if (readOnly) {
			return getReadConnection();
		}
		return getWriteConnection();
	}

	@Override
	public Connection getReadConnection() {
		try {
			Connection conn = readOnlyConnections.get();
			if (conn == null) {
				if (readDataSources != null && readDataSources.size() > 0) {
					setReadOnlyConnection(readDataSources
							.get(ThreadLocalRandom.current().nextInt(readDataSources.size())).getConnection());
					return readOnlyConnections.get();
				} else {
					if (isTransReadOnly()) {
						setReadOnlyConnection(dataSource.getConnection());
						return readOnlyConnections.get();
					} else {
						return getWriteConnection();
					}
				}
			} else {
				return conn;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	private void setReadOnlyConnection(Connection conn) {
		try {
			log.debug("slave connection open  {}", Thread.currentThread().getName());
			readOnlyConnections.set(conn);
			initConnect(readOnlyConnections.get());
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Connection getWriteConnection() {
		try {
			Connection conn = connections.get();
			if (conn == null) {
				log.debug("master connection open  {}", Thread.currentThread().getName());
				connections.set(dataSource.getConnection());
				initConnect(connections.get());
				return connections.get();
			} else {
				return conn;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	private void initConnect(Connection conn) throws SQLException {
		if (initConnect != null && initConnect.length() > 0) {
			conn.prepareStatement(initConnect).execute();
		}
	}

	@Override
	public Boolean beginTransaction(boolean readOnly) {
		if (!isTransactioning()) {
			try {
				getWriteConnection().setAutoCommit(false);
				TransactionLocal transactionLocal = transactions.get();
				if (transactionLocal == null) {
					transactions.set(new TransactionLocal(true, readOnly));
				} else {
					transactionLocal.setBegin(true);
					transactionLocal.setReadOnly(readOnly);
				}
				return true;
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalStateException(e);
			}
		} else {
			return false;
		}
	}

	@Override
	public boolean isTransReadOnly() {
		TransactionLocal transactionLocal = transactions.get();
		return transactionLocal != null && transactionLocal.getReadOnly();
	}

	/**
	 * 是否已经开启了事务
	 * 
	 * @return
	 */
	@Override
	public boolean isTransactioning() {
		TransactionLocal transactionLocal = transactions.get();
		return transactionLocal != null && transactionLocal.getBegin();

	}

	@Override
	public void commitTransaction() {
		Connection connection = connections.get();
		if (connection != null) {
			if (isTransactioning()) {
				try {
					log.debug("master connection close  {}", Thread.currentThread().getName());
					connections.remove();
					transactions.remove();
					connection.commit();
					connection.setAutoCommit(true);
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
					throw new IllegalStateException(e);
				}
			} else {
				closeConnection();
			}
		}
	}

	@Override
	public void closeConnection() {
		Connection connection = connections.get();
		if (connection != null && !isTransactioning()) {
			try {
				log.debug("master connection close  {}", Thread.currentThread().getName());
				connections.remove();
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		closeReadconnection();

	}

	private void closeReadconnection() {
		Connection connection = readOnlyConnections.get();
		if (connection != null) {
			try {
				log.debug("slave connection close  {}", Thread.currentThread().getName());
				readOnlyConnections.remove();
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void rollbackTransaction() {
		Connection connection = connections.get();
		if (connection != null) {
			if (isTransactioning()) {
				try {
					log.debug("master connection close  {}", Thread.currentThread().getName());
					connections.remove();
					transactions.remove();
					connection.rollback();
					connection.setAutoCommit(true);
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
					throw new IllegalStateException(e);
				}
			} else {
				closeConnection();
			}
		}
	}

	private DataSource dataSource;

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public ConnectionManager() {
	}

	private List<DataSource> readDataSources;

	public void setReadDataSources(List<DataSource> readDataSources) {
		this.readDataSources = readDataSources;
	}

	public ConnectionManager(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	private static ThreadLocal<TransactionLocal> transactions = new ThreadLocal<TransactionLocal>() {

		@Override
		protected TransactionLocal initialValue() {
			return new TransactionLocal(false, false);
		}

	};
	/**
	 * 主库
	 */
	private static ThreadLocal<Connection> connections = new ThreadLocal<Connection>();
	/**
	 * 读库
	 */
	private static ThreadLocal<Connection> readOnlyConnections = new ThreadLocal<Connection>();

	/**
	 * 得到表中字段的信息
	 * 
	 * @param fds
	 * @return
	 */
	public static Map<String, LinkedHashSet<PropInfo>> getTbinfo(Class<?> clazz) {
		ConcurrentHashMap<String, LinkedHashSet<PropInfo>> tbmps = ENTITY_CACHED.get(clazz);
		if (tbmps == null) {
			Table tb = clazz.getAnnotation(Table.class);
			LinkedHashSet<PropInfo> cls = new LinkedHashSet<PropInfo>();
			String tbnm = clazz.getSimpleName();
			if (tb != null) {
				tbnm = tb.name().trim();
			}
			Field[] fds = clazz.getDeclaredFields();
			for (Field fd : fds) {
				if (!fd.isAnnotationPresent(Transient.class) && !Modifier.isTransient(fd.getModifiers())
						&& !Modifier.isFinal(fd.getModifiers()) && !Modifier.isStatic(fd.getModifiers())) {
					PropInfo info = new PropInfo(fd.getName(), fd.getType());
					if (fd.isAnnotationPresent(Column.class)) {
						Column fdcolumn = fd.getAnnotation(Column.class);
						String cname = fdcolumn.name();
						if (cname != null && !cname.equals("")) {
							info.setCname(cname);
						} else {
							info.setCname(fd.getName());
						}
						if (fd.getType() == String.class || fd.getType().isEnum()) {
							info.setLength(fdcolumn.length());
						}
						if (!fdcolumn.nullable()) {
							info.setIsNotNull(true);
						}
						if (fdcolumn.unique()) {
							info.setIsUnique(true);
						}
					} else {
						info.setCname(fd.getName());
					}
					if (fd.isAnnotationPresent(Id.class)) {
						info.setIsPrimarykey(true);
						if (fd.isAnnotationPresent(GeneratedValue.class)
								&& fd.getAnnotation(GeneratedValue.class).strategy().equals(GenerationType.IDENTITY)) {
							info.setAutoIncreament(true);
						}
					}
					if (fd.isAnnotationPresent(ColumnRule.class)) {
						info.setColumnRule(fd.getAnnotation(ColumnRule.class));
					}
					if (fd.isAnnotationPresent(MyIndex.class)) {
						info.setIndex(fd.getAnnotation(MyIndex.class));
					}
					if (fd.isAnnotationPresent(Lob.class)) {
						info.setIsLob(true);
					}
					if (fd.getType().isEnum()) {
						if (fd.isAnnotationPresent(Enumerated.class)) {
							info.setEnumType(fd.getAnnotation(Enumerated.class).value());
						} else {
							info.setEnumType(EnumType.ORDINAL);
						}
					}
					cls.add(info);
				}
			}
			tbmps = new ConcurrentHashMap<String, LinkedHashSet<PropInfo>>();
			tbmps.put(tbnm, cls);
			ENTITY_CACHED.put(clazz, tbmps);
		}
		return tbmps;
	}

	public void setGenerateDdl(boolean generateDdl) {
		this.generateDdl = generateDdl;
	}

	@Override
	public boolean isGenerateDdl() {
		return generateDdl;
	}

	private String initConnect;

	@Override
	public boolean isShowSql() {
		return showSql;
	}

	public String getInitConnect() {
		return initConnect;
	}

	public void setInitConnect(String initConnect) {
		this.initConnect = initConnect;
	}

	public void setShowSql(boolean showSql) {
		this.showSql = showSql;
	}

	private boolean showSql = false;
	private boolean generateDdl = false;
	/**
	 * 实体类对应的表名和字段信息列表
	 */
	private volatile static ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, LinkedHashSet<PropInfo>>> ENTITY_CACHED = new ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, LinkedHashSet<PropInfo>>>();

	private static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
}
