package com.verizon.oneparser.datacopier.batch.item;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.io.File;

@Slf4j
public class LogsItemUtils {

    public static void createNodeIfNotExists(CuratorFramework curator, String root, String prefix, File directory) {
        String node = root + "/" + prefix + directory.getAbsolutePath();
        try {
            if (curator != null && curator.checkExists().forPath(node) == null) {
                log.info("Creating node " + node);
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(node);
            }
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }
}
