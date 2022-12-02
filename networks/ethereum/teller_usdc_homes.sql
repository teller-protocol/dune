-- USDC Homes Eth Pool
-- borrowed by the pool owners of USDC Homes (Polygon & Mainnet)
with
  accepted_loans as (
    select
      "bidId" as bidId,
      "lender" as lender,
      "evt_block_time" as loan_start_date
    from
      teller_finance_v2."TellerV2_evt_AcceptedBid"
  ),
  active_loan_details as (
    select
      --   submit_bid.*,
      a."loan_start_date",
      submit_bid."output_bidId_" as bid_id,
      submit_bid."_receiver" as reciever_address,
      submit_bid."_APR" as APR,
      submit_bid."_duration" as duration,
      submit_bid."_principal" as principle,
      submit_bid."_marketplaceId" as market_id,
      case
        WHEN "_marketplaceId" = 6 then 'USDC Homes'
      end as pool_name,
      case
        when "_lendingToken" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then "_principal" / 1e18 -- WETH
        when "_lendingToken" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' then "_principal" / 1e6 -- USDC
      end as loan_amount,
      case
        when "_lendingToken" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then 'WETH'
        when "_lendingToken" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' then 'USDC'
      end as lending_token_symbol
    from
      teller_finance_v2."TellerV2_call_submitBid" as submit_bid
      inner join accepted_loans as a on a.bidId = submit_bid."output_bidId_"
    where
      submit_bid.call_success = true
      and "_marketplaceId" = 6
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
      --   a.*,
      a."loan_start_date",
      a."bid_id",
      a."market_id",
      a."pool_name",
      a."apr",
      a."loan_amount",
      a."reciever_address",
      a."lending_token_symbol",
      a."duration",
      b."price",
      b."price" * a."loan_amount" as current_usd_value
    from
      active_loan_details as a
      inner join recent_prices as b on a.lending_token_symbol = b.symbol
  ),
  submit_evt as (
    select
      submit_bid_evt.*,
      submit_bid_evt."borrower" as borrower_address,
      submit_bid_evt."receiver" as reciever_address,
      submit_bid_evt."bidId" as bidId
    from
      teller_finance_v2."TellerV2_evt_SubmittedBid" as submit_bid_evt
      inner join active_loan_details as a on a."bid_id" = submit_bid_evt."bidId" -- where
      --   submit_evt."bidId" = a.bid_id
  )
select
loan_start_date,
  CONCAT(
    '<a target="_blank" href="https://etherscan.io',
    s."borrower_address",
    '">0',
    SUBSTRING(s."borrower_address" :: text, 2, 5),
    '...',
    SUBSTRING(s."borrower_address" :: text, 39, 42),
    '</a>'
  ) as borrower,
    CONCAT(
    '<a target="_blank" href="https://etherscan.io',
     s."reciever_address",
    '">0',
    SUBSTRING( s."reciever_address" :: text, 2, 5),
    '...',
    SUBSTRING( s."reciever_address" :: text, 39, 42),
    '</a>'
  ) as reciever,
  a.market_id,
  pool_name,
  APR / 1e3 as apr,
  loan_amount,
  lending_token_symbol as token,
  duration / 86400 as loan_duration,
  count(bid_id) as active_loans_count,
  sum("current_usd_value") as "total_usd_value"
from
  accepted_bids_with_amount_current_usd_value a
  inner join submit_evt as s on s.bidId = a.bid_id
group by
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,9
order by
  sum("current_usd_value") desc