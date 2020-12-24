package tech.hongshen.clickhouse.common;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static tech.hongshen.clickhouse.common.ClickhouseConstants.*;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class ConnectConfig implements Serializable {

    private static final long serialVersionUID = 8769893930165013897L;
    public static final String HOST_DELIMITER = ", ";

    private final List<String> hostsAndPorts;
    private final String userName;
    private final String password;
    private final String credentials;

    private int currentHostId = 0;

    public ConnectConfig(Properties parameters) {
        Preconditions.checkNotNull(parameters);
        String hostsString = parameters.getProperty(INSTANCES);
        Preconditions.checkNotNull(hostsString);
        hostsAndPorts = buildHostsAndPort(hostsString);
        Preconditions.checkArgument(hostsAndPorts.size() > 0);
        userName = parameters.getProperty(USERNAME, "default");
        password = parameters.getProperty(PASSWORD, "");
        credentials = buildCredentials(userName, password);
    }

    private static List<String> buildHostsAndPort(String hostsString) {
        return Arrays.stream(hostsString
                .split(HOST_DELIMITER))
                .map(ConnectConfig::checkHttpAndAdd)
                .collect(Collectors.toList());
    }

    private static String checkHttpAndAdd(String host) {
        String newHost = host.replace(" ", "");
        if (!newHost.contains("http")) {
            return "http://" + newHost;
        }
        return newHost;
    }

    private static String buildCredentials(String user, String password) {
        Base64.Encoder x = Base64.getEncoder();
        String credentials = String.join(":", user, password);
        return new String(x.encode(credentials.getBytes()));
    }

    public String getRandomHostUrl() {
        currentHostId = ThreadLocalRandom.current().nextInt(hostsAndPorts.size());
        return hostsAndPorts.get(currentHostId);
    }

    public String getCredentials() {
        return credentials;
    }

}
