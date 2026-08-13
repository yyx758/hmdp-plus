package com.hmdp.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SeckillOrderStatusDTOTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldSerializeOrderIdAsStringToAvoidJavaScriptPrecisionLoss() throws Exception {
        long orderId = 624389400461139857L;
        SeckillOrderStatusDTO status = new SeckillOrderStatusDTO()
                .setOrderId(orderId)
                .setExistingOrderId(orderId)
                .setStatus("SUCCESS");

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(status));

        assertThat(json.get("orderId").isTextual()).isTrue();
        assertThat(json.get("orderId").asText()).isEqualTo(String.valueOf(orderId));
        assertThat(json.get("existingOrderId").isTextual()).isTrue();
        assertThat(json.get("existingOrderId").asText()).isEqualTo(String.valueOf(orderId));
    }
}
