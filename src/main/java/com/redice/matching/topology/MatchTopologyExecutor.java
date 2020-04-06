package com.redice.matching.topology;

import java.util.concurrent.ExecutionException;

public interface MatchTopologyExecutor {
    String deployAfMatchTopology(String appName, String matchTopicX, String matchTopicY, int expectedMsgsCount) throws ExecutionException, InterruptedException;
}
