package com.redice.matching.topology;

import com.redice.matching.entities.MatchRequestJoiner;

public interface MatchTopologyExecutor {

    void deployMatchTopology(String appName, MatchRequestJoiner matchRequestJoiner);

}
