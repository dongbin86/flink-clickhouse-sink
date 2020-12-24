package tech.hongshen.clickhouse.common;

/**
 * @author hongshen
 * @date 2020/12/24
 */

public class ClickhouseConstants {
    // clickhouse table name
    public static final String TARGET_TABLE_NAME = "table-name";
    // how many rows in one request at most
    public static final String BATCH_SIZE = "batch-size";
    //instances example: ip1:8123,ip2:8123
    public static final String INSTANCES = "clickhouse-instances";
    public static final String USERNAME = "clickhouse-user";
    public static final String PASSWORD = "clickhouse-password";
    //flush interval in seconds
    public static final String FLUSH_INTERVAL = "flush-interval-sec";

}
