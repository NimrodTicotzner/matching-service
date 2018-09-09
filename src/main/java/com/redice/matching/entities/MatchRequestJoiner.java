package com.redice.matching.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MatchRequestJoiner {

    @JsonProperty("join_time")
    long joinTime;

    @JsonProperty("keys_joiner")
    Map<String, Joiner> keysJoiner;

    public Map<String, Joiner> getKeysJoiner() {
        return keysJoiner;
    }

    public void setKeysJoiner(Map<String, Joiner> keysJoiner) {
        this.keysJoiner = keysJoiner;
    }

    public long getJoinTime() {
        return joinTime;
    }

    public void setJoinTime(long joinTime) {
        this.joinTime = joinTime;
    }
}
