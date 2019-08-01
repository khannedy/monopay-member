package com.monopay.wallet.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SaveBalanceEvent {

  private String id;

  private String merchantId;

  private Long balance;

  private Long point;

}
