package com.redice.matching.services;

import com.redice.matching.entities.MatchRequest;
import com.redice.matching.entities.MatchRequestJoiner;

public interface KeysGenerator {

    String composeComplexKey(MatchRequestJoiner matchRequestJoiner, MatchRequest matchRequest);

    String composeHashedKey(String v);
}
