-- more details on WHOS approving loans
select
  distinct lender as address,
  get_ens(lender) as ens,
  get_labels(lender) as labels
from
  teller_finance_v2_ethereum.TellerV2_evt_AcceptedBid
