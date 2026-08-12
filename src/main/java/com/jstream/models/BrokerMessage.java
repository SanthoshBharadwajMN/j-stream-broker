package com.jstream.models;

public record BrokerMessage(Long offset, String payload) {
}
