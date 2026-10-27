package com.zmt.picturebackendpro.service.redis;

import com.zmt.picturebackendpro.config.LikeRedisProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LikeRedisKeyFactoryTest {

    @Test
    void sameTargetMustAlwaysUseSameShardAndHashTag() {
        LikeRedisProperties properties = new LikeRedisProperties();
        properties.setKeyPrefix("picture:like:v3");
        properties.setShardCount(16);
        LikeRedisKeyFactory factory = new LikeRedisKeyFactory(properties);

        int first = factory.shardFor("POST", 2112391798298521601L);
        int second = factory.shardFor("POST", 2112391798298521601L);
        Assertions.assertEquals(first, second);

        String usersKey = factory.likedUsersKey("POST", 2112391798298521601L);
        String countKey = factory.likeCountKey("POST", 2112391798298521601L);
        String streamKey = factory.streamKey(first);
        String hashTag = factory.shardPrefix(first).substring(factory.shardPrefix(first).indexOf('{'));

        Assertions.assertTrue(usersKey.contains(hashTag));
        Assertions.assertTrue(countKey.contains(hashTag));
        Assertions.assertTrue(streamKey.contains(hashTag));
        Assertions.assertEquals(factory.idempotencyKey("POST", 2112391798298521601L, "request-1"),
                factory.idempotencyKey("POST", 2112391798298521601L, "request-1"));
    }

    @Test
    void differentRequestIdsMustCreateDifferentIdempotencyKeys() {
        LikeRedisProperties properties = new LikeRedisProperties();
        LikeRedisKeyFactory factory = new LikeRedisKeyFactory(properties);
        Assertions.assertNotEquals(
                factory.idempotencyKey("PICTURE", 10L, "request-A"),
                factory.idempotencyKey("PICTURE", 10L, "request-B"));
    }
}
