package tech.hongshen.clickhouse.common;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Properties;

/**
 *
 * @author hongshen
 * @date 2020/12/24
 *
 * */

import static tech.hongshen.clickhouse.common.ClickhouseConstants.FLUSH_INTERVAL;

public class ClickHouseConfig implements Serializable {

    private static final long serialVersionUID = -2932592417524793016L;
    private final ConnectConfig connectConfig;

    private final int flushInterval;

    public ClickHouseConfig(Properties params) {
        this.connectConfig = new ConnectConfig(params);
        this.flushInterval = Integer.parseInt(params.getProperty(FLUSH_INTERVAL, "2"));
        Preconditions.checkArgument(flushInterval > 0);
    }

    public ConnectConfig getConnectConfig() {
        return connectConfig;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

}
