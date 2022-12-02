WITH
  arcade AS (
    SELECT
      "call_block_time",
      "loanId",
      "lender",
      "borrower",
      "durationdays",
      "principal",
      (("apr" * "principal") / ("principal")) / ("durationdays") * 365 AS APR,
      "loan_currency",
      CASE
        WHEN "loan_currency" = 'ETH' THEN "principal" * eth_price."eth_price"
        ELSE "principal"
      END AS loan_value_usd,
      "rollover",
      "call_block_time" + interval '1' day * "durationdays" AS due_date,
      "contract_address",
      eth_price."eth_price" AS daily_eth_price,
      "call_tx_hash" AS tx_hash
    FROM
      (
        SELECT
          "call_block_time",
          date_trunc('day', "call_block_time") AS day,
          "loanId",
          "lender",
          "borrower",
          "durationsecs" / 86400 AS durationdays,
          "contract_address",
          "call_tx_hash",
          CASE
            WHEN "currency" :: text = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' :: text THEN 'ETH'
            WHEN "currency" :: text = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' :: text THEN 'USDC'
            WHEN "currency" :: text = '0x6b175474e89094c44da98b954eedeac495271d0f' :: text THEN 'DAI'
            ELSE "currency" :: text
          END AS loan_currency,
          CASE
            WHEN "currency" :: text = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' :: text THEN "principal" / 1e18
            WHEN "currency" :: text = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' :: text THEN "principal" / 1e6
            ELSE NULL
          END AS principal,
          "rollover",
          apr
        FROM
          (
            SELECT
              "call_block_time",
              "loanId",
              "lender",
              "borrower",
              "contract_address",
              "call_tx_hash",
              created_qv."durationsecs",
              created_qv."currency",
              created_qv."principal",
              (
                (created_qv."interest" / created_qv."principal") / (created_qv."durationsecs" / 86400) * 365
              ) * 100 AS APR,
              'No' AS Rollover
            FROM
              (
                SELECT
                  *
                FROM
                  pawnfi."LoanCore_call_startLoan"
              ) AS sv
              LEFT JOIN (
                SELECT
                  CAST(cv.terms ->> 'durationSecs' AS numeric(999, 1)) AS durationSecs,
                  CAST(cv.terms ->> 'interest' AS numeric(999, 1)) AS interest,
                  CAST(cv.terms ->> 'principal' AS numeric(999, 1)) AS principal,
                  cv.terms ->> 'payableCurrency' AS currency,
                  cv."evt_tx_hash",
                  'No' AS Rollover
                FROM
                  pawnfi."LoanCore_evt_LoanCreated" cv
              ) AS created_qv on sv."call_tx_hash" = created_qv."evt_tx_hash"
            UNION
            (
              SELECT
                "call_block_time",
                created_qv2."loanId",
                "lender",
                "borrower",
                "contract_address",
                "call_tx_hash",
                created_qv2."durationsecs",
                created_qv2."currency",
                created_qv2."principal",
                (
                  (created_qv2."interest" / created_qv2."principal") / (created_qv2."durationsecs" / 86400) * 365
                ) * 100 AS APR,
                'No' AS Rollover
              FROM
                (
                  SELECT
                    *
                  FROM
                    pawnfi_v2."LoanCore_call_startLoan"
                ) AS sv2
                LEFT JOIN (
                  SELECT
                    CAST(cv2.terms ->> 'durationSecs' AS numeric(999, 1)) AS durationSecs,
                    CAST(cv2.terms ->> 'interest' AS numeric(999, 1)) AS interest,
                    CAST(cv2.terms ->> 'principal' AS numeric(999, 1)) AS principal,
                    cv2.terms ->> 'payableCurrency' AS currency,
                    cv2."loanId",
                    cv2."evt_tx_hash",
                    'No' AS Rollover
                  FROM
                    pawnfi_v2."LoanCore_evt_LoanCreated" cv2
                ) AS created_qv2 on sv2."call_tx_hash" = created_qv2."evt_tx_hash"
            )
            /*V2.01 CONTRACT*/
            UNION
            SELECT
              "call_block_time",
              "output_loanId" AS loanId,
              "lender",
              "borrower",
              "contract_address",
              "call_tx_hash",
              CAST(terms ->> 'durationSecs' AS numeric(999, 1)) AS durationSecs,
              terms ->> 'payableCurrency' AS currency,
              CAST(terms ->> 'principal' AS numeric(999, 1)) AS principal,
              CAST(terms ->> 'interestRate' AS numeric(999, 1)) / 1e20 AS apr,
              'No' AS Rollover
            FROM
              pawnfi_v201."LoanCore_call_startLoan"
              /*rollovers*/
            UNION
            SELECT
              "call_block_time",
              "output_newLoanId" AS loanId,
              "lender",
              "borrower",
              "contract_address",
              "call_tx_hash",
              CAST(terms ->> 'durationSecs' AS numeric(999, 1)) AS durationSecs,
              terms ->> 'payableCurrency' AS currency,
              CAST(terms ->> 'principal' AS numeric(999, 1)) AS principal,
              CAST(terms ->> 'interestRate' AS numeric(999, 1)) / 1e20 AS apr,
              'Yes' AS Rollover
            FROM
              pawnfi_v201."LoanCore_call_rollover"
          ) AS combine
      ) AS convert
      LEFT JOIN (
        SELECT
          date_trunc('day', minute) AS day,
          AVG(price) AS eth_price
        FROM
          prices."layer1_usd"
        WHERE
          "symbol" = 'ETH'
          AND date_trunc('day', minute) > date_trunc('day', now()) - interval '3 years'
        GROUP BY
          1
      ) AS eth_price on convert."day" = eth_price."day"
  )
select
  call_block_time,
  CONCAT(
    '<a target="_blank" href="https://etherscan.io/address',
    tx_hash,
    '">0',
    SUBSTRING(tx_hash :: text, 2, 5),
    '...',
    SUBSTRING(tx_hash :: text, 39, 42),
    '</a>'
  ) as tx_hash,
    CONCAT(
    '<a href="https://etherscan.io/address/',
    concat(0,substring(contract_address :: text FROM 2)), '" target="_blank">URL 🌐  </a>' 
   ) as contract_address,
  loan_value_usd,
  due_date,
  lender,
  borrower,
  durationdays,
  principal,
  apr,
  loan_currency
FROM
  arcade
order by
  1 desc