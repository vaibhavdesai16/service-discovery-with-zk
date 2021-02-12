package com.sysx;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Hello world!
 *
 */
public class LeaderElection implements  Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static final String ELECTION_NAMESPACE = "/election";
    private String cuurentZnodeName;
    ZooKeeper zooKeeper;

    public void main() throws IOException, KeeperException, InterruptedException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from zookeeper, exiting application ");
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name " + znodeFullPath);
        this.cuurentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");

    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        Stat predecessorStat = null;
        String predecessorZnodeName ="";
        while (predecessorStat == null){
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);
            if (smallestChild.equals(cuurentZnodeName)) {
                System.out.println("I am the leader " + cuurentZnodeName);
                return;
            } else {
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children, cuurentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" +predecessorZnodeName, this);
            }
        }
        System.out.println("watching znode " + predecessorZnodeName);
        System.out.println();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("disconnected from zk event");
                        zooKeeper.notifyAll();
                    }
                }
                break;

            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException e) {

                } catch (InterruptedException e) {

                }
                break;
        }
    }
}
