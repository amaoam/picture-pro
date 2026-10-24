package com.zmt.picturebackendpro.service.impl;

import com.zmt.picturebackendpro.model.entity.CommunityLikeOutboxEvent;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 不连接数据库的单元测试：验证同一目标不能越过退避事件或死信。 */
class CommunityLikeOutboxProcessorOrderTest {

    @Test
    void shouldStopOneTargetAtFutureRetryButContinueOtherTargets() {
        List<CommunityLikeOutboxEvent> input = Arrays.asList(
                event(10L, "POST", 100L, 0, true, 1),
                event(11L, "POST", 100L, 0, false, -1),
                event(12L, "POST", 100L, 0, true, 1),
                event(13L, "PICTURE", 200L, 0, true, 1));

        List<CommunityLikeOutboxEvent> result = CommunityLikeOutboxProcessor.filterProcessablePrefix(input);

        assertEquals(Arrays.asList(10L, 13L), ids(result));
    }

    @Test
    void deadLetterMustBlockLaterEventsForSameTarget() {
        List<CommunityLikeOutboxEvent> input = Arrays.asList(
                event(20L, "POST", 100L, 0, true, 1),
                event(21L, "POST", 100L, 2, false, -1),
                event(22L, "POST", 100L, 0, true, 1),
                event(23L, "POST", 101L, 0, true, 1));

        List<CommunityLikeOutboxEvent> result = CommunityLikeOutboxProcessor.filterProcessablePrefix(input);

        assertEquals(Arrays.asList(20L, 23L), ids(result));
    }

    @Test
    void shouldSortInputBeforeApplyingTargetPrefixRules() {
        List<CommunityLikeOutboxEvent> input = Arrays.asList(
                event(32L, "POST", 100L, 0, true, -1),
                event(30L, "POST", 100L, 0, true, 1),
                event(31L, "POST", 100L, 0, true, -1));

        List<CommunityLikeOutboxEvent> result = CommunityLikeOutboxProcessor.filterProcessablePrefix(input);

        assertEquals(Arrays.asList(30L, 31L, 32L), ids(result));
    }

    private static CommunityLikeOutboxEvent event(Long id, String targetType, Long targetId,
                                                    int status, boolean retryDue, int delta) {
        CommunityLikeOutboxEvent event = new CommunityLikeOutboxEvent();
        event.setId(id);
        event.setTargetType(targetType);
        event.setTargetId(targetId);
        event.setStatus(status);
        event.setRetryDue(retryDue);
        event.setDelta(delta);
        return event;
    }

    private static List<Long> ids(List<CommunityLikeOutboxEvent> events) {
        return events.stream().map(CommunityLikeOutboxEvent::getId).collect(Collectors.toList());
    }
}
