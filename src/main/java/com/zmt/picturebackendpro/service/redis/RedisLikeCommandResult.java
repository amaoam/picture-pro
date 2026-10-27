package com.zmt.picturebackendpro.service.redis;

import lombok.Data;


/** Redis Lua 返回的固定结构；字段名与 Lua cjson.encode 的字段保持一致。 */
@Data
public class RedisLikeCommandResult {
    private String outcome;
    private String error;
    private Boolean liked;
    private Long likeCount;
    private Boolean changed;
    private Boolean duplicate;
    private String eventId;
    private Long pairVersion;

}
