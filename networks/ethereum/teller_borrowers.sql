-- more details on WHOS requesting loans
select
  distinct borrower as address,
  get_labels(borrower) as labels
from
  teller_finance_v2_ethereum.TellerV2_evt_SubmittedBid
order by
  labels desc