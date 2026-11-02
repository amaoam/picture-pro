package com.zmt.picturebackendpro.service.impl.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将 RabbitMQ 死信原文与诊断信息持久化到 MySQL，供后续检索和人工处理。
 *
 * <p>职责：</p>
 * <ol>
 *   <li>接收死信审计消费者投递过来的原始 {@link Message}；</li>
 *   <li>从消息体中尽力解析出可检索字段（eventId、requestId、targetType 等）；</li>
 *   <li>从消息头读取重试次数、死信原因、异常类、异常消息、死信时间等诊断信息；</li>
 *   <li>将消息原文（文本 + Base64）、消息头 JSON、诊断字段统一写入
 *       {@code community_like_dead_letter_audit} 表；</li>
 *   <li>按 {@code message_id} 做 UPSERT，重复投递时累加 {@code delivery_count}，
 *       避免重复插入；</li>
 *   <li>返回只包含可安全写日志的定位字段的 {@link AuditSummary}。</li>
 * </ol>
 *
 * <p>设计原则：</p>
 * <ul>
 *   <li><b>不丢原文</b>：即使消息体 JSON 损坏或 messageId 缺失，也通过 SHA-256 兜底
 *       生成稳定 ID，并完整保存原始字节（Base64），保证消息可追溯；</li>
 *   <li><b>字段截断</b>：所有可能超长的字段都截断到表结构允许的长度，避免写库失败；</li>
 *   <li><b>头信息安全序列化</b>：消息头可能包含 byte[]、嵌套 Map/List 等非 JSON 原生类型，
 *       序列化前统一做安全转换；</li>
 *   <li><b>不返回敏感正文</b>：返回值仅含定位字段，避免完整正文进入日志。</li>
 * </ul>
 *
 * <p>通过 {@link ConditionalOnProperty} 限定只在 {@code community.like.mode=rabbitmq} 时装配。</p>
 */
@Service
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
public class RabbitMqLikeDeadLetterAuditService {

    /** Spring JDBC 模板：负责执行死信审计 UPSERT SQL */
    @Resource
    private JdbcTemplate jdbcTemplate;

    /** JSON 序列化/反序列化器：解析消息体、序列化消息头 */
    @Resource
    private ObjectMapper objectMapper;

    /** 点赞 MQ 配置：提供主队列名与死信队列名，用于审计表来源/目标字段 */
    @Resource
    private RabbitMqLikeProperties properties;

    /**
     * 保存一条死信审计记录。
     *
     * <p>message_id 唯一，重复投递使用 UPSERT 统计 delivery_count，避免重复插入。
     * 返回值只包含可安全写入日志的定位字段，不返回完整消息正文。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>解析消息体（尽力解析为 JSON，失败时 payload 为 null，但原文仍会保存）；</li>
     *   <li>从消息体提取 eventId、requestId、targetType、targetId、userId、desiredState、delta；</li>
     *   <li>确定稳定的 messageId：优先使用消息头 messageId；为空时用 body 的 SHA-256 兜底；
     *       超长时再用 SHA-256 压缩；</li>
     *   <li>从消息头读取重试次数、死信原因、异常类、异常消息、死信时间；</li>
     *   <li>保存消息原文（UTF-8 文本 + Base64）与安全序列化后的消息头 JSON；</li>
     *   <li>执行 UPSERT：新消息插入并置状态 {@code PENDING_REVIEW}，重复消息累加 delivery_count。</li>
     * </ol>
     *
     * @param message 死信原始消息
     * @return 审计摘要（仅含定位字段，可安全写日志）
     */
    public AuditSummary persist(Message message) {
        byte[] body = message.getBody() == null ? new byte[0] : message.getBody();
        MessageProperties messageProperties = message.getMessageProperties();
        Map<String, Object> headers = messageProperties.getHeaders();

        // 尽力从消息体解析出可检索字段；解析失败时 payload 为 null，但原文仍会保存
        JsonNode payload = parsePayload(body);
        String eventId = truncate(jsonText(payload, "eventId"), 128);
        String requestId = truncate(jsonText(payload, "requestId"), 128);
        String targetType = truncate(jsonText(payload, "targetType"), 16);
        String targetId = truncate(jsonText(payload, "targetId"), 64);
        String userId = truncate(jsonText(payload, "userId"), 64);
        String desiredState = truncate(jsonText(payload, "desiredState"), 16);
        String delta = truncate(jsonText(payload, "delta"), 8);

        // 确定稳定的 messageId：优先用消息头；缺失时用 body 的 SHA-256 兜底，保证重投时同一条消息 ID 一致
        String messageId = trimToNull(messageProperties.getMessageId());
        if (messageId == null) messageId = "malformed-" + sha256(body);
        // 超长 messageId 再用 SHA-256 压缩，避免超过表字段长度
        if (messageId.length() > 128) messageId = "oversize-" + sha256(messageId.getBytes(StandardCharsets.UTF_8));

        // 从消息头读取诊断信息；缺失时给出合理的默认值
        int retryCount = intHeader(headers.get("x-like-retry-count"), 0);
        String reasonCode = headerText(headers, "x-like-dead-letter-reason");
        // 无显式原因码时：若存在 x-death 说明是 broker 端死信，否则标记为未知原因
        if (reasonCode == null) reasonCode = headers.containsKey("x-death")
                ? "BROKER_DEAD_LETTER" : "UNKNOWN_DEAD_LETTER_REASON";
        reasonCode = truncate(reasonCode, 128);
        String errorClass = truncate(headerText(headers, "x-like-dead-letter-error-class"), 255);
        String errorMessage = headerText(headers, "x-like-dead-letter-error-message");
        errorMessage = truncate(errorMessage, 2048);
        // 死信时间戳缺失时用当前时间兜底，保证字段非空
        long deadLetteredAtMs = longHeader(headers.get("x-like-dead-lettered-at"), System.currentTimeMillis());

        // 原文保存两份：UTF-8 文本便于检索，Base64 保证二进制/损坏内容也能完整留存
        String payloadText = new String(body, StandardCharsets.UTF_8);
        String payloadBase64 = Base64.getEncoder().encodeToString(body);
        // 消息头统一安全序列化，避免 byte[]、嵌套 Map/List 等非 JSON 原生类型导致序列化失败
        String headersJson = serializeHeaders(headers);

        // UPSERT：message_id 唯一，重复投递时累加 delivery_count 并更新诊断字段
        // - COALESCE(VALUES(x), x)：新值为 NULL 时保留旧值，避免因解析失败覆盖已有可检索字段
        // - GREATEST(...)：retry_count 与 dead_lettered_at_ms 取较大值，反映最严重的重试情况
        // - delivery_count+1：统计同一条消息被重复投递的次数
        String sql = "INSERT INTO community_like_dead_letter_audit (" +
                "message_id,event_id,request_id,target_type,target_id,user_id,desired_state,delta," +
                "retry_count,reason_code,error_class,error_message,source_queue,dead_queue," +
                "dead_lettered_at_ms,payload_text,payload_base64,headers_json,delivery_count," +
                "first_seen_at,last_seen_at,review_status) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,CURRENT_TIMESTAMP(3),CURRENT_TIMESTAMP(3),'PENDING_REVIEW') " +
                "ON DUPLICATE KEY UPDATE " +
                "event_id=COALESCE(VALUES(event_id),event_id), " +
                "request_id=COALESCE(VALUES(request_id),request_id), " +
                "target_type=COALESCE(VALUES(target_type),target_type), " +
                "target_id=COALESCE(VALUES(target_id),target_id), " +
                "user_id=COALESCE(VALUES(user_id),user_id), " +
                "desired_state=COALESCE(VALUES(desired_state),desired_state), " +
                "delta=COALESCE(VALUES(delta),delta), " +
                "retry_count=GREATEST(retry_count,VALUES(retry_count)), " +
                "reason_code=VALUES(reason_code),error_class=VALUES(error_class),error_message=VALUES(error_message), " +
                "dead_lettered_at_ms=GREATEST(dead_lettered_at_ms,VALUES(dead_lettered_at_ms)), " +
                "payload_text=VALUES(payload_text),payload_base64=VALUES(payload_base64),headers_json=VALUES(headers_json), " +
                "delivery_count=delivery_count+1,last_seen_at=CURRENT_TIMESTAMP(3)";

        jdbcTemplate.update(sql,
                messageId, eventId, requestId, targetType, targetId, userId, desiredState, delta,
                retryCount, reasonCode, errorClass, errorMessage,
                // source_queue / dead_queue 记录消息来源与死信目标，便于排查链路
                properties.getQueue(), properties.getDeadQueue(),
                deadLetteredAtMs, payloadText, payloadBase64, headersJson);

        // 只返回定位字段，避免完整消息正文进入调用方日志
        return new AuditSummary(messageId, eventId, targetType, targetId, reasonCode, retryCount);
    }

    /**
     * 尽力把消息体解析为 JSON 树。
     *
     * <p>解析失败不抛异常，返回 null。这样即使消息体损坏，后续流程仍会保存原始字节
     * 与消息头，保证死信不丢失。</p>
     *
     * @param body 消息体字节
     * @return JSON 树；解析失败返回 null
     */
    private JsonNode parsePayload(byte[] body) {
        try {
            return objectMapper.readTree(body);
        } catch (Exception ignored) {
            // 即使消息 JSON 损坏，也必须保存原始字节和消息头
            return null;
        }
    }

    /**
     * 从 JSON 树中取指定字段的文本值。
     *
     * @param node  JSON 树，可为 null（表示消息体解析失败）
     * @param field 字段名
     * @return 字段文本值；节点/字段为 null 时返回 null
     */
    private String jsonText(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) return null;
        return value.asText();
    }

    /**
     * 从消息头读取文本值，统一转为字符串。
     *
     * @param headers 消息头
     * @param key     头键名
     * @return 头值的字符串形式；不存在时返回 null
     */
    private String headerText(Map<String, Object> headers, String key) {
        Object value = headers.get(key);
        return value == null ? null : String.valueOf(value);
    }

    /**
     * 从消息头读取 int 值，解析失败或缺失时返回默认值。
     *
     * @param value        原始头值
     * @param defaultValue 默认值
     * @return 解析后的 int 或默认值
     */
    private int intHeader(Object value, int defaultValue) {
        try {
            return value == null ? defaultValue : Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    /**
     * 从消息头读取 long 值，解析失败或缺失时返回默认值。
     *
     * @param value        原始头值
     * @param defaultValue 默认值
     * @return 解析后的 long 或默认值
     */
    private long longHeader(Object value, long defaultValue) {
        try {
            return value == null ? defaultValue : Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    /**
     * 把消息头安全序列化为 JSON 字符串。
     *
     * <p>AMQP 消息头的值类型比较自由（可能包含 byte[]、嵌套 Map/List、自定义对象等），
     * 直接交给 Jackson 可能序列化失败或产生不可读结构。这里先递归转换成 JSON 安全的值，
     * 再进行序列化。序列化失败时返回 {@code "{}"}，保证写库不因头信息异常而失败。</p>
     *
     * @param headers 消息头
     * @return JSON 字符串；序列化失败返回 "{}"
     */
    private String serializeHeaders(Map<String, Object> headers) {
        Map<String, Object> safe = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            safe.put(entry.getKey(), safeHeaderValue(entry.getValue()));
        }
        try {
            return objectMapper.writeValueAsString(safe);
        } catch (JsonProcessingException ignored) {
            return "{}";
        }
    }

    /**
     * 递归地把任意消息头值转换为 JSON 安全值。
     *
     * <p>规则：</p>
     * <ul>
     *   <li>null / String / Number / Boolean：原样保留；</li>
     *   <li>byte[]：Base64 编码为字符串，避免二进制无法序列化；</li>
     *   <li>Map：递归处理键值，键统一转字符串；</li>
     *   <li>List：递归处理每个元素；</li>
     *   <li>其他对象：退化为 {@code String.valueOf}。</li>
     * </ul>
     *
     * @param value 原始头值
     * @return JSON 安全值
     */
    private Object safeHeaderValue(Object value) {
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof byte[]) return Base64.getEncoder().encodeToString((byte[]) value);
        if (value instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            ((Map<?, ?>) value).forEach((key, item) -> result.put(String.valueOf(key), safeHeaderValue(item)));
            return result;
        }
        if (value instanceof List) {
            List<Object> result = new ArrayList<>();
            for (Object item : (List<?>) value) result.add(safeHeaderValue(item));
            return result;
        }
        return String.valueOf(value);
    }

    /**
     * 去除首尾空白；为 null 或全空白时返回 null。
     *
     * @param value 原始字符串
     * @return 去空白后的字符串或 null
     */
    private String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) return null;
        return value.trim();
    }

    /**
     * 截断字符串到指定最大长度，避免超出数据库字段长度导致写库失败。
     *
     * @param value     原始字符串，可为 null
     * @param maxLength 最大长度
     * @return 截断后的字符串；value 为 null 时返回 null
     */
    private String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) return value;
        return value.substring(0, maxLength);
    }

    /**
     * 计算字节数组的 SHA-256 十六进制摘要。
     *
     * <p>用于在 messageId 缺失或超长时生成稳定的兜底 ID，保证同一条消息在多次重投时
     * 生成相同的 ID，从而让 UPSERT 的 delivery_count 能正确累加。</p>
     *
     * @param bytes 输入字节
     * @return 64 位小写十六进制摘要
     * @throws IllegalStateException JVM 不支持 SHA-256（实际不可能）
     */
    private String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder result = new StringBuilder(64);
            for (byte b : digest) result.append(String.format("%02x", b & 0xff));
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("当前 JVM 不支持 SHA-256", impossible);
        }
    }

    /**
     * 死信审计摘要。
     *
     * <p>仅包含可安全写入日志的定位字段（messageId、eventId、targetType、targetId、
     * reasonCode、retryCount），不包含消息正文或异常堆栈，避免敏感信息泄漏。</p>
     */
    public static final class AuditSummary {
        /** 稳定消息 ID（消息头 messageId，缺失时由 body 的 SHA-256 兜底） */
        private final String messageId;
        /** 事件 ID（从消息体解析） */
        private final String eventId;
        /** 目标类型（如 PICTURE / POST 等） */
        private final String targetType;
        /** 目标对象 ID */
        private final String targetId;
        /** 死信原因码（如 MAX_RETRIES_EXCEEDED / BROKER_DEAD_LETTER / UNKNOWN_DEAD_LETTER_REASON） */
        private final String reasonCode;
        /** 已重试次数 */
        private final int retryCount;

        public AuditSummary(String messageId, String eventId, String targetType, String targetId,
                            String reasonCode, int retryCount) {
            this.messageId = messageId;
            this.eventId = eventId;
            this.targetType = targetType;
            this.targetId = targetId;
            this.reasonCode = reasonCode;
            this.retryCount = retryCount;
        }

        public String getMessageId() { return messageId; }
        public String getEventId() { return eventId; }
        public String getTargetType() { return targetType; }
        public String getTargetId() { return targetId; }
        public String getReasonCode() { return reasonCode; }
        public int getRetryCount() { return retryCount; }
    }
}