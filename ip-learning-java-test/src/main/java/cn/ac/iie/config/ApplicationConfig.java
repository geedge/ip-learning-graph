package cn.ac.iie.config;


import cn.ac.iie.utils.ConfigUtils;

public class ApplicationConfig {

    public static final String ARANGODB_HOST = ConfigUtils.getStringProperty( "arangoDB.host");
    public static final Integer ARANGODB_PORT = ConfigUtils.getIntProperty("arangoDB.port");
    public static final String ARANGODB_USER = ConfigUtils.getStringProperty( "arangoDB.user");
    public static final String ARANGODB_PASSWORD = ConfigUtils.getStringProperty( "arangoDB.password");
    public static final String ARANGODB_DB_NAME = ConfigUtils.getStringProperty( "arangoDB.DB.name");
    public static final Integer ARANGODB_TTL = ConfigUtils.getIntProperty( "arangoDB.ttl");
    public static final Integer ARANGODB_BATCH = ConfigUtils.getIntProperty( "arangoDB.batch");

    public static final Integer UPDATE_ARANGO_BATCH =ConfigUtils.getIntProperty("update.arango.batch");

    public static final Integer THREAD_POOL_NUMBER = ConfigUtils.getIntProperty( "thread.pool.number");
    public static final Integer THREAD_AWAIT_TERMINATION_TIME = ConfigUtils.getIntProperty( "thread.await.termination.time");

    public static final Long READ_CLICKHOUSE_MAX_TIME = ConfigUtils.getLongProperty("read.clickhouse.max.time");
    public static final Long READ_CLICKHOUSE_MIN_TIME = ConfigUtils.getLongProperty("read.clickhouse.min.time");

    public static final Integer TIME_LIMIT_TYPE = ConfigUtils.getIntProperty("time.limit.type");
    public static final Integer UPDATE_INTERVAL = ConfigUtils.getIntProperty("update.interval");

    public static final Integer DISTINCT_CLIENT_IP_NUM = ConfigUtils.getIntProperty("distinct.client.ip.num");
    public static final Integer RECENT_COUNT_HOUR = ConfigUtils.getIntProperty("recent.count.hour");


}
