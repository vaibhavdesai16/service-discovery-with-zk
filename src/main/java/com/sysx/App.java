package com.sysx;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.main();
    }
}
