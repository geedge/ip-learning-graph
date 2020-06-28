package cn.ac.iie.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ClickhouseConnect {
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseConnect.class);
    private static DruidDataSource dataSource = null;
    private static ClickhouseConnect dbConnect = null;
    private static Properties props = new Properties();

    static {
        getDbConnect();
    }

    private static void getDbConnect() {
        try {
            if (dataSource == null) {
                dataSource = new DruidDataSource();
                props.load(ClickhouseConnect.class.getClassLoader().getResourceAsStream("clickhouse.properties"));
                //设置连接参数
                dataSource.setUrl("jdbc:clickhouse://" + props.getProperty("db.id"));
                dataSource.setDriverClassName(props.getProperty("drivers"));
                dataSource.setUsername(props.getProperty("mdb.user"));
                dataSource.setPassword(props.getProperty("mdb.password"));
                //配置初始化大小、最小、最大
                dataSource.setInitialSize(Integer.parseInt(props.getProperty("initialsize")));
                dataSource.setMinIdle(Integer.parseInt(props.getProperty("minidle")));
                dataSource.setMaxActive(Integer.parseInt(props.getProperty("maxactive")));
                //配置获取连接等待超时的时间
                dataSource.setMaxWait(30000);
                //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
                dataSource.setTimeBetweenEvictionRunsMillis(2000);
                //防止过期
                dataSource.setValidationQuery("SELECT 1");
                dataSource.setTestWhileIdle(true);
                dataSource.setTestOnBorrow(true);
                dataSource.setKeepAlive(true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());

        }
    }

    /**
     * 数据库连接池单例
     *
     * @return dbConnect
     */
    public static synchronized ClickhouseConnect getInstance() {
        if (null == dbConnect) {
            dbConnect = new ClickhouseConnect();
        }
        return dbConnect;
    }

    /**
     * 返回druid数据库连接
     *
     * @return 连接
     * @throws SQLException sql异常
     */
    public DruidPooledConnection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 清空PreparedStatement、Connection对象，未定义的置空。
     *
     * @param pstmt      PreparedStatement对象
     * @param connection Connection对象
     */
    public void clear(Statement pstmt, Connection connection) {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }

    }

    public ResultSet executorQuery(String query,Connection connection,Statement pstm){
//        Connection connection = null;
//        Statement pstm = null;
        try {
            connection = getConnection();
            pstm = connection.createStatement();
            return pstm.executeQuery(query);
        }catch (Exception e){
            LOG.error(e.getMessage());
            return null;
        }
    }

}
