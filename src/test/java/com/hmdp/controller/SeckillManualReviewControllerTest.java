package com.hmdp.controller;

import com.hmdp.dto.SeckillManualRetryDTO;
import com.hmdp.dto.UserDTO;
import com.hmdp.service.SeckillAcceptedOrderRecoveryService;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SeckillManualReviewControllerTest {

    private final SeckillAcceptedOrderRecoveryService recoveryService =
            Mockito.mock(SeckillAcceptedOrderRecoveryService.class);

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void operatorApiIsDisabledWithoutConfiguredToken() {
        SeckillManualReviewController controller =
                new SeckillManualReviewController(recoveryService, "");

        ResponseStatusException error = assertThrows(ResponseStatusException.class,
                () -> controller.retry(1001L, "any", request()));

        assertEquals(HttpStatus.NOT_FOUND, error.getStatus());
        Mockito.verifyNoInteractions(recoveryService);
    }

    @Test
    void operatorApiRequiresTokenAndLogin() {
        SeckillManualReviewController controller =
                new SeckillManualReviewController(recoveryService, "secret");

        ResponseStatusException wrongToken = assertThrows(ResponseStatusException.class,
                () -> controller.retry(1001L, "wrong", request()));
        ResponseStatusException noLogin = assertThrows(ResponseStatusException.class,
                () -> controller.retry(1001L, "secret", request()));

        assertEquals(HttpStatus.FORBIDDEN, wrongToken.getStatus());
        assertEquals(HttpStatus.UNAUTHORIZED, noLogin.getStatus());
        Mockito.verifyNoInteractions(recoveryService);
    }

    @Test
    void authorizedOperatorActionsAreAuditedByService() {
        SeckillManualReviewController controller =
                new SeckillManualReviewController(recoveryService, "secret");
        UserDTO user = new UserDTO();
        user.setId(9L);
        UserHolder.saveUser(user);

        controller.retry(1001L, "secret", request());
        controller.confirmCompleted(1001L, "secret", request());
        controller.requestFailureReview(1001L, "secret", request());

        Mockito.verify(recoveryService).retryManualReview(1001L, 9L, "checked");
        Mockito.verify(recoveryService).confirmCompleted(1001L, 9L, "checked");
        Mockito.verify(recoveryService).requestFailureReview(1001L, 9L, "checked");
    }

    private SeckillManualRetryDTO request() {
        SeckillManualRetryDTO dto = new SeckillManualRetryDTO();
        dto.setReason("checked");
        return dto;
    }
}
