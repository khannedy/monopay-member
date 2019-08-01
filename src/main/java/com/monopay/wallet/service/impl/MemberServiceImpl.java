package com.monopay.wallet.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.monopay.wallet.entity.Balance;
import com.monopay.wallet.entity.Member;
import com.monopay.wallet.model.service.CreateMemberServiceRequest;
import com.monopay.wallet.model.service.GetMemberServiceRequest;
import com.monopay.wallet.model.service.UpdateMemberServiceRequest;
import com.monopay.wallet.model.web.response.CreateMemberWebResponse;
import com.monopay.wallet.model.web.response.GetMemberWebResponse;
import com.monopay.wallet.model.web.response.UpdateMemberWebResponse;
import com.monopay.wallet.repository.BalanceRepository;
import com.monopay.wallet.repository.MemberRepository;
import com.monopay.wallet.service.MemberService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.util.UUID;

@Slf4j
@Service
@Validated
public class MemberServiceImpl implements MemberService {

  @Autowired
  private MemberRepository memberRepository;

  @Autowired
  private BalanceRepository balanceRepository;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private ObjectMapper objectMapper;

  @Override
  public CreateMemberWebResponse create(@Valid CreateMemberServiceRequest request) {
    Member member = Member.builder()
      .id(UUID.randomUUID().toString())
      .email(request.getEmail())
      .phone(request.getPhone())
      .merchantId(request.getMerchantId())
      .name(request.getName())
      .verified(Boolean.FALSE)
      .build();

    member = memberRepository.save(member);
    publishMember(member);

    return CreateMemberWebResponse.builder()
      .id(member.getId())
      .name(member.getName())
      .phone(member.getPhone())
      .email(member.getEmail())
      .build();
  }

  @Override
  public UpdateMemberWebResponse update(@Valid UpdateMemberServiceRequest request) {
    Member member = memberRepository.findByIdAndMerchantId(request.getMemberId(), request.getMerchantId());
    member.setName(request.getName());
    member.setVerified(request.getVerified());

    memberRepository.save(member);
    publishMember(member);

    return UpdateMemberWebResponse.builder()
      .id(member.getId())
      .name(member.getName())
      .phone(member.getPhone())
      .email(member.getEmail())
      .build();
  }

  @Override
  public GetMemberWebResponse get(@Valid GetMemberServiceRequest request) {
    Member member = memberRepository.findByIdAndMerchantId(request.getMemberId(), request.getMerchantId());
    Balance balance = balanceRepository.findById(member.getId()).get();

    return GetMemberWebResponse.builder()
      .id(member.getId())
      .name(member.getName())
      .phone(member.getPhone())
      .email(member.getEmail())
      .balance(GetMemberWebResponse.Balance.builder()
        .balance(balance.getBalance())
        .point(balance.getPoint())
        .build())
      .build();
  }

  public void publishMember(Member member) {
    try {
      String payload = objectMapper.writeValueAsString(member);
      kafkaTemplate.send("monopay-save-member-event", payload);
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    }
  }
}
