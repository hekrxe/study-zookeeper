package com.hkr.sz.d;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by tanhuayou on 2018/12/24
 */
public class EmbeddedZookeeper {

    public static void main(String[] args) {
        Thread mainThread = new Thread(() -> {
            Properties properties = new Properties();
            File file = new File(System.getProperty("java.io.tmpdir")
                    + File.separator + UUID.randomUUID());
            file.deleteOnExit();
            properties.setProperty("dataDir", file.getAbsolutePath());
            properties.setProperty("clientPort", "2181");

            QuorumPeerConfig config = new QuorumPeerConfig();
            try {
                config.parseProperties(properties);

                ServerConfig serverConfig = new ServerConfig();
                serverConfig.readFrom(config);
                ZooKeeperServerMain main = new ZooKeeperServerMain();
                main.runFromConfig(serverConfig);

            } catch (IOException | QuorumPeerConfig.ConfigException e) {
                e.printStackTrace();
            }

        });
        mainThread.start();
        try {
            mainThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
