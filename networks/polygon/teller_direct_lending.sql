/* 
 Dune Engine v2 - TV2Primary on Polygon
 USDC Homes Pool Owner on Polygon 0xd44e34c996709b389d6d2d0c145bad254b4b3b74
 Active/Direct Lending (Lender to Borrower)
 */
select
  case
    WHEN submit_call._marketplaceId = 3 then 'USDC Homes'
  end as pool_name,
  case
    when submit_call._lendingToken = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' then submit_call._principal / 1e6 -- USDC
  end as loan_amount,
  case
    when submit_call._lendingToken = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' then 'USDC'
  end as lending_token_symbol,
  submit_evt.borrower,
  accept_evt.lender,
  submit_evt.receiver,
  accept_evt.bidId,
  submit_call._APR / 100 as APR,
  submit_call._duration / 86400 as duration,
  accept_evt.evt_block_time,
  case
    when repay_evt.evt_block_time = null then 'None'
    else repay_evt.evt_block_time
  end as last_payment_date
from
  tv2_polygon.TV2Primary_evt_AcceptedBid as accept_evt
  inner join tv2_polygon.TV2Primary_call_submitBid as submit_call on accept_evt.bidId = submit_call.output_bidId_
  inner join tv2_polygon.TV2Primary_evt_SubmittedBid submit_evt on accept_evt.bidId = submit_evt.bidId
  left join tv2_polygon.TV2Primary_evt_LoanRepayment repay_evt on accept_evt.bidId = repay_evt.bidId
where
  submit_call._marketplaceId = 3 and submit_call._principal / 1e6 > 1000