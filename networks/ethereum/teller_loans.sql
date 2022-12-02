with
  distinct_active_loans as (
    select
      "bidId" as bidId,
      "evt_block_time" as evt_blocktime
    from
      teller_finance_v2."TellerV2_evt_AcceptedBid"
  ),
  active_loan_details as (
    select
      submit_bid_call."_APR",
      submit_bid_call."call_block_number",
      submit_bid_call."call_block_time",
      submit_bid_call."_duration",
      submit_bid_call."call_success",
      submit_bid_call."contract_address",
      submit_bid_call."_lendingToken",
      submit_bid_call."_marketplaceId",
      submit_bid_call."_metadataURI",
      submit_bid_call."output_bidId_",
      submit_bid_call."_principal",
      case
        WHEN "_marketplaceId" = 1 then 'ApeNow'
        WHEN "_marketplaceId" = 2 then 'Polytrade'
        WHEN "_marketplaceId" = 3 then 'UNI-V3 USDC-WETH'
        WHEN "_marketplaceId" = 4 then 'bloom'
        WHEN "_marketplaceId" = 5 then 'ENS Lending Pool'
      end as marketplaceName,
      case
        when "_lendingToken" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then "_principal" / 1e18
        when "_lendingToken" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' then "_principal" / 1e6
      end as loan_amount,
      case
        when "_lendingToken" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then 'WETH'
        when "_lendingToken" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' then 'USDC'
      end as lendingTokenSymbol
    from
      teller_finance_v2."TellerV2_call_submitBid" as submit_bid_call
      inner join distinct_active_loans as a on a.bidId = submit_bid_call."output_bidId_"
    where
      submit_bid_call.call_success = true
  ),
  recent_prices as (
    (
      select
        a."price",
        a."symbol",
        a."decimals"
      from
        prices."usd" as a
      where
        a."symbol" = 'WETH'
      order by
        minute desc
      limit
        1
    )
    UNION
    (
      select
        b."price",
        b."symbol",
        b."decimals"
      from
        prices."usd" as b
      where
        b."symbol" = 'USDC'
      order by
        minute desc
      limit
        1
    )
  ), accepted_bids_with_amount_current_usd_value as (
    select
      a.*,
      a."_duration" as duration,
      a."call_block_time" as call_block_time,
      a."output_bidId_" as bid_id,
      b."price",
      b."price" * a."loan_amount" as current_usd_value
    from
      active_loan_details as a
      inner join recent_prices as b on a.lendingTokenSymbol = b.symbol
  ),
  active_loans as (
    select
      marketplaceName,
      bid_id,
      current_usd_value,
      duration / 86400 as duration_days
      --   count(bid_id),
      --   sum("current_usd_value") as "total_usd_value"
    from
      accepted_bids_with_amount_current_usd_value
    group by
      1,
      2,
      3,
      4
    order by
      current_usd_value desc
  )
select
  a.*,
  submit_evt."borrower",
  submit_evt."bidId",
  submit_evt."evt_block_time" as loan_submitted_date
from
  active_loans a
  join teller_finance_v2."TellerV2_evt_SubmittedBid" submit_evt on submit_evt."bidId" = a.bid_id