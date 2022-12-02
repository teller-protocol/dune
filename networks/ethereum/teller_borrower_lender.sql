/*
 Dune v2 Engine - Teller on Ethereum 
 USDC homes pool owner address on ethereum -  0x462279e5b07f6d24d4faee076df3ad7adcdb426b
 The query below shows the Supplied funds / Passive Lending (Lender to Market Owner)
 */
select
  case
    WHEN submit_bid_call._marketplaceId = 6 then 'USDC Homes'
  end as pool_name,
  case
    when submit_bid_call._lendingToken = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then submit_bid_call._principal / 1e18 -- WETH
    when submit_bid_call._lendingToken = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' then submit_bid_call._principal / 1e6 -- USDC
  end as loan_amount,
  case
    when submit_bid_call._lendingToken = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then 'WETH'
    when submit_bid_call._lendingToken = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' then 'USDC'
  end as lending_token_symbol,
  case
    WHEN submit_bid_evt.borrower = '0x462279e5b07f6d24d4faee076df3ad7adcdb426b' then 'Supplied Funds - Passive Lending'
  end as loan_type,
  submit_bid_evt.bidId,
  submit_bid_evt.borrower,
  accept_evt.lender,
  submit_bid_call._APR / 100 as APR,
  submit_bid_call._duration / 86400 as duration,
  submit_bid_evt.evt_block_time,
  submit_bid_call._lendingToken,
  submit_bid_call._marketplaceId,
  submit_bid_call._principal
from
  teller_finance_v2_ethereum.TellerV2_evt_SubmittedBid as submit_bid_evt
  inner join teller_finance_v2_ethereum.TellerV2_evt_AcceptedBid as accept_evt on submit_bid_evt.bidId = accept_evt.bidId
  inner join teller_finance_v2_ethereum.TellerV2_call_submitBid submit_bid_call on submit_bid_evt.bidId = submit_bid_call.output_bidId_
where
  submit_bid_call._marketplaceId = 6