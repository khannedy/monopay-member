package com.monopay.wallet.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.monopay.wallet.entity.Balance;
import com.monopay.wallet.event.SaveBalanceEvent;
import com.monopay.wallet.repository.BalanceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class BalanceListener {

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private BalanceRepository balanceRepository;

  @KafkaListener(topics = "monopay-save-balance-event")
  public void onSaveMemberEvent(String payload) throws IOException {
    log.info(payload);
    SaveBalanceEvent event = objectMapper.readValue(payload, SaveBalanceEvent.class);
    if (!balanceRepository.existsById(event.getId())) {
      Balance balance = Balance.builder()
        .id(event.getId())
        .merchantId(event.getMerchantId())
        .point(event.getPoint())
        .balance(event.getBalance())
        .build();
      balanceRepository.save(balance);
    } else {
      Balance balance = balanceRepository.findById(event.getId()).get();
      balance.setBalance(event.getBalance());
      balance.setPoint(event.getPoint());
      balanceRepository.save(balance);
    }
  }

}
