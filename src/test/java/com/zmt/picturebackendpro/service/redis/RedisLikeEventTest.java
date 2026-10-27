package com.zmt.picturebackendpro.service.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RedisLikeEventTest {

    @Test
    void nullableOwnerShouldRemainNull() {
        RedisLikeEvent event = new RedisLikeEvent();
        event.setOwnerUserId("");
        Assertions.assertNull(event.ownerUserIdAsLongOrNull());
        event.setOwnerUserId("123");
        Assertions.assertEquals(123L, event.ownerUserIdAsLongOrNull());
    }

    @Test
    void bigIdsAreParsedFromStringsWithoutDoubleConversion() {
        RedisLikeEvent event = new RedisLikeEvent();
        event.setTargetId("2112391798298521601");
        Assertions.assertEquals(2112391798298521601L, event.targetIdAsLong());
    }
}
