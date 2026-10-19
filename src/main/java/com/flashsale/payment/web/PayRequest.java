package com.flashsale.payment.web;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import com.flashsale.payment.PaymentProvider;

record PayRequest(@NotNull PaymentProvider provider, @NotBlank @Size(max = 100) String paymentToken) {
}
