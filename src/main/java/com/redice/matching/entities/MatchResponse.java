package com.redice.matching.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MatchResponse {

    @JsonProperty("first_user_id")
    private String firstUserId;
    @JsonProperty("first_user_socket_id")
    private String firstUserSocketId;
    @JsonProperty("first_user_level")
    private String firstUserLevel;
    @JsonProperty("second_user_id")
    private String secondUserId;
    @JsonProperty("second_user_socket_id")
    private String secondUserSocketId;
    @JsonProperty("second_user_level")
    private String secondUserLevel;

    public String getFirstUserId() {
        return firstUserId;
    }

    public void setFirstUserId(String firstUserId) {
        this.firstUserId = firstUserId;
    }

    public String getFirstUserSocketId() {
        return firstUserSocketId;
    }

    public void setFirstUserSocketId(String firstUserSocketId) {
        this.firstUserSocketId = firstUserSocketId;
    }

    public String getFirstUserLevel() {
        return firstUserLevel;
    }

    public void setFirstUserLevel(String firstUserLevel) {
        this.firstUserLevel = firstUserLevel;
    }

    public String getSecondUserId() {
        return secondUserId;
    }

    public void setSecondUserId(String secondUserId) {
        this.secondUserId = secondUserId;
    }

    public String getSecondUserSocketId() {
        return secondUserSocketId;
    }

    public void setSecondUserSocketId(String secondUserSocketId) {
        this.secondUserSocketId = secondUserSocketId;
    }

    public String getSecondUserLevel() {
        return secondUserLevel;
    }

    public void setSecondUserLevel(String secondUserLevel) {
        this.secondUserLevel = secondUserLevel;
    }
}
