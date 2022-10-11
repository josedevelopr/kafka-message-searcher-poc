package com.rdlnbank.customerapp.api.model;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder
public class TransactionRequest implements Serializable {
  private String id;
  private String transactionType;
  private String bankAccountNumber;
  private String token;
  private boolean bankAssurance;
  private Long amount;
  private String serviceId;
}
