/* 
* Description: 
* Protocols, loan (start / end) date, amt, apy, interests and revenues
* Note this is a hodge-podge of others queries unioned together and is a WIP
* 1. TrueFi 
* 2. Goldfinch
* 3. Maple
* 4. Clearpool
* 5. Ribbon
*/
WITH
  borrows as (
    SELECT
      "evt_tx_hash",
      "evt_block_time",
      'v1' as version,
      "fee" as fee,
      'tfTUSD' as pool
    FROM
      truefi."TrueFiPool_evt_Borrow"
    WHERE
      evt_block_time < '2021-05-29'
    UNION ALL
    SELECT
      "evt_tx_hash",
      "evt_block_time",
      'v2' as version,
      0 as fee,
      "pool" as pool
    FROM
      truefi."TrueLender2_evt_Funded"
  ),
  transfers as (
    SELECT
      CONCAT('\x', SUBSTRING(t."to" :: text, 3)) as loan_token,
      (t.value + b.fee) / 10 ^ d.decimals as loan_amount,
      d."symbol" as token,
      t.evt_block_time as loan_issued_timestamp,
      t."to" as borrower,
      t.evt_tx_hash,
      b.fee / 10 ^ d.decimals as fee,
      b.version --, t."from" as "loanFactory"
    FROM
      erc20."ERC20_evt_Transfer" t
      JOIN borrows b on t.evt_tx_hash = b.evt_tx_hash
      JOIN erc20."tokens" d on d."contract_address" = t."contract_address"
      AND d."contract_address" IN (
        '\x0000000000085d4780b73119b644ae5ecd22b376',
        -- TUSD smart contract
        '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        -- USDC smart contract
        '\xdac17f958d2ee523a2206206994597c13d831ec7',
        -- USDT smart contract
        '\x4fabb145d64652a948d72533023f6e7a623c7c53' -- BUSD smart contract
      )
    WHERE
      t."from" IN (
        '\x16d02Dc67EB237C387023339356b25d1D54b0922',
        -- TrueFi LoanFactory lender address
        '\x23ade98FA576AcBab49A67d2E6d4159B89eE26b9',
        -- TrueFi USDC LoanFactory lender address
        '\xa606dd423df7dfb65efe14ab66f5fdebf62ff583' -- TrueLender2 contract
      )
  ),
  repays as (
    SELECT
      r."to" as borrower,
      d.decimals,
      d."symbol" as token,
      SUM(r."value") / 10 ^ d.decimals as repay_amount,
      MAX(r.evt_block_time) as repay_timestamp
    FROM
      erc20."ERC20_evt_Transfer" r
      JOIN transfers t on r."to" = t.borrower
      JOIN erc20."tokens" d on d.symbol = t.token
      AND d."contract_address" IN (
        '\x0000000000085d4780b73119b644ae5ecd22b376' -- TUSD smart contract
,
        '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' -- USDC smart contract
,
        '\xdac17f958d2ee523a2206206994597c13d831ec7' -- USDT smart contract
,
        '\x4fabb145d64652a948d72533023f6e7a623c7c53' -- BUSD smart contract
      )
    WHERE
      r."from" NOT IN (
        '\x16d02Dc67EB237C387023339356b25d1D54b0922' -- TrueFi LoanFactory lender address
,
        '\x23ade98FA576AcBab49A67d2E6d4159B89eE26b9' -- TrueFi USDC LoanFactory lender address
,
        '\xa606dd423df7dfb65efe14ab66f5fdebf62ff583' -- TrueLender2 contract
      )
    GROUP BY
      r."to",
      d."symbol",
      d.decimals
  ),
  loans as (
    SELECT
      t.loan_token,
      t.loan_amount,
      t.token,
      t.version,
      MAX(t.loan_issued_timestamp) as loan_issued_timestamp --, t.evt_tx_hash
,
      MAX(r.repay_timestamp) as loan_repaid_timestamp,
      EXTRACT(
        DAY
        FROM
          MAX(r.repay_timestamp) - MAX(t.loan_issued_timestamp)
      ) as duration,
      CASE
        WHEN SUM(repay_amount - loan_amount) > 0 then 'REPAID'
        WHEN SUM(repay_amount - loan_amount) < 0 then 'RESTRUCTURED/DEFAULTED'
        ELSE 'ACTIVE'
      END as status,
      SUM(r.repay_amount) as repay_amount --, t.version
,
      CASE
        WHEN t.version = 'v1' then SUM(t.fee)
        WHEN t.version = 'v2' then COALESCE(SUM(repay_amount - loan_amount) * 0.1, 0)
        else 0
      end as fee,
      COALESCE(SUM(repay_amount - loan_amount), 0) interest_paid,
      CASE
        WHEN t.version = 'v1' then SUM(repay_amount - loan_amount)
        WHEN t.version = 'v2' then COALESCE(SUM(repay_amount - loan_amount) * 0.9, 0)
        else 0
      end as net_interest_paid_to_lenders --, DATE_TRUNC('month', MAX(t.loan_issued_timestamp)) as loan_issued_month
      /*, (SUM(repay_amount - loan_amount)
       /SUM(loan_amount))*(360/DATE_PART('day', MAX(repay_timestamp) - MAX(loan_issued_timestamp))
       as interest_rate*/
    FROM
      transfers t
      left JOIN repays r on r.borrower = t.borrower
    WHERE
      loan_amount > 1 -- exclude 1 TUSD test loan
    GROUP BY
      1,
      2,
      3,
      4
  ) -- Cash in each pools
,
  transfers_cash AS (
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      - tr.value AS amount,
      'tfBUSD' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."from" = '\x1Ed460D149D48FA7d91703bf4890F97220C09437'
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      tr.value AS amount,
      'tfBUSD' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."to" = '\x1Ed460D149D48FA7d91703bf4890F97220C09437'
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      - tr.value AS amount,
      'tfUSDC' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."from" = '\xA991356d261fbaF194463aF6DF8f0464F8f1c742'
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      tr.value AS amount,
      'tfUSDC' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."to" = '\xA991356d261fbaF194463aF6DF8f0464F8f1c742'
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      - tr.value AS amount,
      'tfUSDT' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."from" = '\x6002b1dcB26E7B1AA797A17551C6F487923299d7'
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      tr.value AS amount,
      'tfUSDT' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."to" = '\x6002b1dcB26E7B1AA797A17551C6F487923299d7'
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      - tr.value AS amount,
      'tfTUSD' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."from" = '\x97cE06c3e3D027715b2d6C22e67D5096000072E5'
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      contract_address AS token_address,
      tr.value AS amount,
      'tfTUSD' AS pool
    FROM
      erc20."ERC20_evt_Transfer" tr
    WHERE
      tr."to" = '\x97cE06c3e3D027715b2d6C22e67D5096000072E5'
  ),
  active_loans as (
    SELECT
      loan_token,
      CASE
        WHEN loan_token = '\x576e0971e642f7a0cfd45c85564e7a4a060e5889' THEN 'Nibbio'
        WHEN loan_token = '\x966607729de0f60d878a76e29998321f0b6f5f5e' THEN 'TPS Capital/Three Arrows Capital'
        WHEN loan_token = '\x93f4cbc396d467860b8b0d0af624353226f73957' THEN 'Sixtant'
        WHEN loan_token = '\xc0449d79cf710336cdac35c53480aa261b1ecaef' THEN 'Folkvang'
        WHEN loan_token = '\xcf3dbc132b7f539cef349d82da5cc70262f04395' THEN 'mgnr.io'
        WHEN loan_token = '\x483c37a40f78263491c11ef1e1c7921a22612af0' THEN 'mgnr.io'
        WHEN loan_token = '\x73e7a62e5c88aed26df4d1df4d11a41029103278' THEN 'TrueTrading'
        WHEN loan_token = '\x9997d247ac85af038cfe49d0ca393b90bd664e19' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x4a66a867f52df4ed1d8580a1c383b2dd036a3c47' THEN 'Blockwater Technologies'
        WHEN loan_token = '\x244b3e163ecb30d7c2ba6400d7dbb9db60b8f0af' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x97688fb5a06b6f3d3b524f7530afbeb883cdc642' THEN 'Invictus Capital'
        WHEN loan_token = '\x7ef385b496dd044571626df38c2e5cfa301ebe91' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x2dbe894e4f211b5a5e994ecfa1fb7b3958c7bbff' THEN 'Wintermute Trading'
        WHEN loan_token = '\xefd1db0584be794ba8aa226f089e1ca5fec967e3' THEN 'Nascent'
        WHEN loan_token = '\x3a1e022d3d5e779b40b6e002d8f6cb03eebf7456' THEN 'Bastion Trading'
        WHEN loan_token = '\x84b58a1493fc9fa11cfcccebceb1a32c31358f53' THEN 'Bastion Trading'
        WHEN loan_token = '\xdb2340b90ab9de4167a3f92e874dcb41c69baf26' THEN 'Auros'
        WHEN loan_token = '\x52164096a1b43193e6a73d2ef3c99ee66e081ef8' THEN 'TPS Capital/Three Arrows Capital'
        WHEN loan_token = '\x61e40b979545bf47b159e5a1902083e5bcda2b0d' THEN 'Amber Group'
        WHEN loan_token = '\x050ead578e7658d5e1481b94c8dd47300f9e8497' THEN 'Amber Group'
        WHEN loan_token = '\xb1ee0fe2b342ccdbe87f2b90225edce47fa152e6' THEN 'Nibbio'
        WHEN loan_token = '\x35a75bf242a0ae1449dc406633dbaf744303d963' THEN 'Nibbio'
        WHEN loan_token = '\x6eedc3df6747ebc1c00ab254b64c46c9e07e02f1' THEN 'mgnr.io'
        WHEN loan_token = '\xf0974bb470037dcbdf5e0001caa883e84589d76f' THEN 'Sixtant'
        WHEN loan_token = '\x3abf1c66f1745c3084a0b5d26bd6a303f95ad888' THEN 'Folkvang'
        WHEN loan_token = '\xae2ca89f180a21c573f6199a9be2be738c2e5a5e' THEN 'Blockwater Technologies'
        WHEN loan_token = '\x34d46856d3c4813ed4bb460faac54d7af85b8218' THEN 'Hodlnaut'
        WHEN loan_token = '\x3efd53f9accf0b5b206616f12aca1625494f3c8e' THEN 'Jump Crypto'
        WHEN loan_token = '\xe22a15f9fb6899d78c90ab8d867f6144590823fa' THEN 'Nascent'
        WHEN loan_token = '\xd425b404a8ba51dc4b260fdbb284d55b7a970a13' THEN 'Wintermute Trading'
        WHEN loan_token = '\x9f96cb5e9ae96d4a816cc7251dc92ea0288f90f3' THEN 'Bastion Trading'
        WHEN loan_token = '\x62154424f0a00c401d251c29315ece49402505db' THEN 'FBG Group'
        WHEN loan_token = '\x112ca50b78faebcaeb70d6c23238438c3f003b7f' THEN 'Vexil Capital'
        WHEN loan_token = '\xba9a013a11b6b83651619e8435d81dfe272ff438' THEN 'Borrower 0x819'
        WHEN loan_token = '\x7ee274f8f1fb2ec9a6a623952fc43d4abf6bf56f' THEN 'Borrower 0xB60'
        WHEN loan_token = '\x5575f75e14cf81630e8d57b0662f5e7da161c543' THEN 'Ovex'
        WHEN loan_token = '\xb64f045c30bf4b2c2baf85fd40e68d249b077249' THEN 'Nibbio'
        WHEN loan_token = '\x60f13a5ade319a2117d0a07155fd566c1008b9f1' THEN 'TrueTrading'
        WHEN loan_token = '\x900b1fac27e052d613713a846953321a1df52931' THEN 'Amber Group'
        WHEN loan_token = '\x53460c95b169a29a07e029ca1180d5d6d4a35e52' THEN 'Wavebridge'
        WHEN loan_token = '\x6692d345bfaaf689b552cee466fc3afb411edbfe' THEN 'mgnr.io'
        WHEN loan_token = '\xe02c823bdbb528683347519062426e1049ed1f7e' THEN 'Babel Finance'
        WHEN loan_token = '\x7b785b0dab2c37a07a307882c68cdbcff5301cdb' THEN 'Folkvang'
        WHEN loan_token = '\x63f828f9490ea9a1ca6965bfc7eb386fb0faa6c4' THEN 'Vexil Capital'
        WHEN loan_token = '\x0d0ec8e87663b1f28c2f01feb3f3eb3ae3ebdea1' THEN 'Akuna'
        WHEN loan_token = '\xf4d1a929e979d9af8e365b550d49878351bc4c21' THEN 'Blockwater Technologies'
        WHEN loan_token = '\xf590649cbacdb392658d4436f2328768c9f2caaa' THEN 'Sixtant'
        WHEN loan_token = '\x60430491093672fe82c446ab265ffcf2cca57289' THEN 'Subspace Capital'
        WHEN loan_token = '\x70235e50138e191d0e2000587c29f0bb6f75d1d5' THEN 'Wintermute Trading'
        WHEN loan_token = '\x67f3ba00e32e45c06699db47e1a95f3c346d45dd' THEN 'Wintermute Trading'
        WHEN loan_token = '\x6bf376317339048c2e08248f5e506f22e37e21ba' THEN 'GSR Markets Ltd.'
        WHEN loan_token = '\x1ec2f4419c7f50ad06ee37adb3a0685e74bb202d' THEN 'Borrower 0xB60'
        WHEN loan_token = '\x8768f87b31c9b97c35f21b4149ed70e678957b9d' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xafb79df39e1b898df00f0ac71e96dd7b7dc8bb97' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x479e75d870034815aeae0a208244fdef3b360e35' THEN 'Celsius Network'
        WHEN loan_token = '\xa893f5a2daddd9436dbd146357c4f5f60e277f0e' THEN 'Celsius Network'
        WHEN loan_token = '\x8182bafc4f497b230e345ab6c7727899f11b7708' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xfd15de660d6e2161fc6faa59a9a13a7ed7f09ba7' THEN 'Kronos Holdings Ltd'
        WHEN loan_token = '\x5c85267b7cd1003a711c7780467571444e17a796' THEN 'Plutus Lending LLC'
        WHEN loan_token = '\x37b8010decf23bd0ab0dc88cd971ce2912d8964c' THEN 'Folkvang'
        WHEN loan_token = '\x23b2ce78fd1cfd9eb93e9f334403583c36dc6a5a' THEN 'Wintermute Trading'
        WHEN loan_token = '\xfa5016f1fcc7544e3d2218aa921854790ba2b19a' THEN 'Nascent'
        WHEN loan_token = '\x30c0f87059f8bcb12e3a26e7d0fc793bf3655209' THEN 'Bastion Trading'
        WHEN loan_token = '\xbe24afc4b5bb70959707f92664ab07f76278f9cd' THEN 'FBG Group'
        WHEN loan_token = '\x309d8b391835dacc934bda4af5cc1d8f196ba122' THEN 'Ovex'
        WHEN loan_token = '\x839e6149d255509fb73b6cbc93a0e56645e0924e' THEN 'Invictus Capital'
        WHEN loan_token = '\xef7fd5d0610c69330cd56630c35104e4609f495a' THEN 'Celsius Network'
        WHEN loan_token = '\x2386113397433f39a52ff55fe7dd944cefa42860' THEN 'Sixtant'
        WHEN loan_token = '\x34363065f873fa73317f5b6012d404dad71fbf0b' THEN 'Akuna'
        WHEN loan_token = '\x7ed7fd3d98bc8f5f80c2a43440a3d75b419f7fc2' THEN 'GSR Markets Ltd.'
        WHEN loan_token = '\x033e25dde1cfdb4f5183e924d4ff93b678cf8f74' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x4f6e5b76cf44eed21c300da2a88c7882ad633879' THEN 'Borrower 0xB60'
        WHEN loan_token = '\x4d67e84fe63a42cfdaac124267fe2ce7956773b9' THEN 'FBG Group'
        WHEN loan_token = '\x1828d69a359511a6a6c8e430389ac6c8d1d6659d' THEN 'Nascent'
        WHEN loan_token = '\x456aa76cc82a0ca1fa176813b8158c57aeb1fa0e' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xfc8e0dac74777944fece276d8708b637faa3bbcf' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xb1f7f008615fe6950cb12c6b913d284de71db92c' THEN 'Plutus Lending LLC'
        WHEN loan_token = '\xa1c6654aef1cdbce9d007a56c18abf6f0475bf8b' THEN 'Nibbio'
        WHEN loan_token = '\xadf046a62c1a1a01289176a015dff0995bba46e9' THEN 'Amber Group'
        WHEN loan_token = '\x54e378719f8fa599e8a028a8b136612fa055e272' THEN 'Amber Group'
        WHEN loan_token = '\x949fcd96ea1110ebf56a0210b1f7b40049276eda' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x518fa7b5b1d077dfbe532920ba3c1bbff9695bcb' THEN 'mgnr.io'
        WHEN loan_token = '\x2614135a17f987df581369208dbf00aa84b762d7' THEN 'Folkvang'
        WHEN loan_token = '\xd9ab354982bd4bf859def0a6df38e42327fe894d' THEN 'Chater Legend'
        WHEN loan_token = '\xdd66b11a87f499b3a76f3472dbfb8284a84f082e' THEN 'Wavebridge'
        WHEN loan_token = '\xbdf4d951dfd5805516586b77aca4c946784d897c' THEN 'Blockwater Technologies'
        WHEN loan_token = '\x17314ce9a99a98be1a2537af925b488499c10102' THEN 'Wintermute Trading'
        WHEN loan_token = '\x07e4bbfeb5b79828a1e304ab971a964e03a9306d' THEN 'Wintermute Trading'
        WHEN loan_token = '\x92279a7816899b13ca23b42d2b8baa6c9cf73702' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x5634f8536bd921388fc39b817dcb090109a531e3' THEN 'Ovex'
        WHEN loan_token = '\xdb7754d0ca2c62896bf1f745288f7008f13ba599' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x9fb208196fa3505f531efd7d89712f5e18d96a5d' THEN 'mgnr.io'
        WHEN loan_token = '\xd6268be0e38909a41ca5ae5a72336d996df22d70' THEN 'Subspace Capital'
        WHEN loan_token = '\x9161b6817a16c22cb37158e8d90ac07916ad3e59' THEN 'Bastion Trading'
        WHEN loan_token = '\x855c27009066fb8d0cf3af2a753ad074373d5e87' THEN 'Folkvang'
        WHEN loan_token = '\x164c6f974a9dab180bc763412624107e413af8a2' THEN 'Nibbio'
        WHEN loan_token = '\x7b55a11789501f415f666423e6d1bb02e8f257c3' THEN 'Wintermute Trading'
        WHEN loan_token = '\x8ab3055722c62be1673a194cc9c7a0fbdd898eca' THEN 'Wavebridge'
        WHEN loan_token = '\xd380b40baf1bcb01394b8fb9619c8de32c1af94c' THEN 'Blockwater Technologies'
        WHEN loan_token = '\x97ce37ab516722c7eeb699a5b1cb6417feb6bfe2' THEN 'Invictus Capital'
        WHEN loan_token = '\xb1a8ebc9a0e1af0c055173e017640d6251b83b36' THEN 'Wintermute Trading'
        WHEN loan_token = '\x4fb2d8104706bef93eebef8dbe549e53add5185f' THEN 'Amber Group'
        WHEN loan_token = '\xb6aa9ab4555a4a5fe1e111c6a9330ad07d8c98fd' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x864641e8040798da2e680f4bb3c93ed6ebb62982' THEN 'Ovex'
        WHEN loan_token = '\x30eb1e7fb76ae6feda7efb1bc6a4d3490f2e61d2' THEN 'Kbit'
        WHEN loan_token = '\xc09c3aac0cb40f40eb70e1f4f4246d077622b6ca' THEN 'Invictus Capital'
        WHEN loan_token = '\xfd73553fac90fb32f91f8f98bf6e2d913add613d' THEN 'Nibbio'
        WHEN loan_token = '\xc36660970370a8a7ddec02e9c8b431db6d1493c8' THEN 'Folkvang'
        WHEN loan_token = '\xa2c6c6622fe3f1ed20ba764639fffd8aed752ee1' THEN 'mgnr.io'
        WHEN loan_token = '\x4e5c7f20269c7fd87cf0e158fd3f1e5067514826' THEN 'Wintermute Trading'
        WHEN loan_token = '\x059523485954f652a161df4890e77040551e9bc1' THEN 'Kbit'
        WHEN loan_token = '\x86a814ec8392bc29e7b63dab15e66cdd7d9b8f6d' THEN 'Subspace Capital'
        WHEN loan_token = '\xcf8b3878c3744eeff35f71c4bfb42fa1e4ceb26d' THEN 'Bastion Trading'
        WHEN loan_token = '\x5663fc294978f857739a61c6dbc86e1c2a4cc565' THEN 'Poloniex'
        WHEN loan_token = '\x583f674b8e2c36807e7371b2d27849f0e98549cc' THEN 'Amber Group'
        WHEN loan_token = '\xdbdd2dcef7ede35284183baff021e47a1e89b4d7' THEN 'Multicoin Capital'
        WHEN loan_token = '\xcab2f2a8ab011ed7cd74fb77c49f057d9f90c2a7' THEN 'Nibbio'
        WHEN loan_token = '\xa6be941081f36abf11fd76f7957aefa2370f77a2' THEN 'Grapefruit Trading'
        WHEN loan_token = '\xcf17b60d0c3900433cff5170c62f0a63d01ab3be' THEN 'Wintermute Trading'
        WHEN loan_token = '\xc424fc688809800dd293c6f1818efeb131b019a1' THEN 'Invictus Capital'
        WHEN loan_token = '\xf93a6b4b28ffe825099566e0d058162cc568937d' THEN 'Folkvang'
        WHEN loan_token = '\x1e2097e34bbb8c5f2ba791018224394edac12ce9' THEN 'Bastion Trading'
        WHEN loan_token = '\x7ee183b6d0278b8641e735097d9fafa67934509c' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xade453ea96cb7edaa3de72b59616aed59cccc5eb' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xe2531311b00f19b0da74491900cc2959bf2e6745' THEN 'Nibbio'
        WHEN loan_token = '\x643dc313835f9773d865fc3ebc09e366210bb5e8' THEN 'Grapefruit Trading'
        WHEN loan_token = '\xe98c493112f8776aff3bfd0b7cd3816251e7cc29' THEN 'Invictus Capital'
        WHEN loan_token = '\x3431587892b7acc501122bccf5c5933cc31a2934' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x24682b0cef4836ee5539b7eaba0f16b9990e7ab0' THEN 'Wintermute Trading'
        WHEN loan_token = '\x9a30c20cfee80cb7c201ae86d6559fb4bd1c3f57' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x3344fc6db44382ac5c0a7ccdef2a3dd5db8a4229' THEN 'Grapefruit Trading'
        WHEN loan_token = '\xa22f04a6b4ff923c9db3a0f75429d82eb3787ccf' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x24aabf488d14de3404726a386fe1650650c513c3' THEN 'Invictus Capital'
        WHEN loan_token = '\x6ed5f68f11826f36e12d134845032bd8f53b4e22' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xa42f00db95be452c59efac6292294d6b44b500c3' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\xa2fee3736f85145ad33d4d834886df8042bcbd66' THEN 'Alameda Research Ltd.'
        WHEN loan_token = '\x7206bef38daa7188c22271436017c044d477bacc' THEN 'TrustToken Test'
        WHEN loan_token = LOWER('\x48790efE3eE930cbc08Ce22c6E36de62B3f28b43') THEN 'Folkvang'
        WHEN loan_token = LOWER('\xFDDb19de2aaFa3D26D10e864db2fDf0dDbDDcdd8') THEN 'Nibbio'
        WHEN loan_token = LOWER('\xDDDca5CF3A53868428Ea428cdC6B30BCA0F05921') THEN 'Alameda Research Ltd.'
        WHEN loan_token = LOWER('\xc16E912Fb424F69Db4d59227A50E3c5213d3879c') THEN 'TrueTrading Asset Management'
        WHEN loan_token = LOWER('\xf07817B54721cee98cb39972F6318652d2256C52') THEN 'TrueTrading Asset Management'
        WHEN loan_token = LOWER('\x7372D690D521f289467aC9C0B647C5D35e8c6D3c') THEN 'mgnr.io'
        WHEN loan_token = LOWER('\xcCfCeb165345e9C5231cF1465aa8e956DC28E2fB') THEN 'Folkvang'
        WHEN loan_token = LOWER('\x5b70B2A333987B0aaf238c68010B094674cd7F35') THEN 'Auros'
        ELSE 'OTHER'
      END as borrower,
      loan_amount,
      token,
      loan_issued_timestamp,
      loan_repaid_timestamp,
      status,
      fee as fee_to_stakers,
      net_interest_paid_to_lenders,
      version
    FROM
      loans
    WHERE
      token in ('USDC', 'TUSD', 'USDT', 'BUSD')
    order by
      loan_issued_timestamp desc
  ),
  apy_tvl as (
    SELECT
      _term as term,
      _pool as pool,
      _apy / 10000 as apy,
      lfe."contractAddress" as loan_token
    FROM
      truefi."LoanFactory2_call_createLoanToken" lf
      LEFT JOIN truefi."LoanFactory2_evt_LoanTokenCreated" lfe on lf."call_tx_hash" = lfe."evt_tx_hash"
  ),
  final_pool_dao AS(
    SELECT
      cast(m.loan_token as text),
      borrower,
      token,
      cast(loan_issued_timestamp as text),
      cast(loan_repaid_timestamp as text),
      cast(
        loan_issued_timestamp + term / 24 / 3600 * INTERVAL '1 Days' as text
      ) as loan_end_timestamp,
      loan_amount,
      CASE
        WHEN status = 'REPAID' then loan_amount * apy * (term / 24 / 3600 / 365)
        WHEN status = 'ACTIVE' then loan_amount * apy * (
          DATE_PART(
            'day',
            NOW() :: timestamp - loan_issued_timestamp :: timestamp
          ) / 365
        )
        ELSE 0
      END AS projected_interest,
      CASE
        WHEN status = 'REPAID' then 0
        ELSE loan_amount * (
          1 + apy * (
            DATE_PART(
              'day',
              NOW() :: timestamp - loan_issued_timestamp :: timestamp
            ) / 365
          )
        )
      END AS tvl,
      apy,
      status,
      fee_to_stakers,
      0.0025 * loan_amount as establishment_fees
    FROM
      active_loans m
      left JOIN apy_tvl at on m.loan_token = cast(at.loan_token as text)
  ),
  cash_balance AS (
    SELECT
      cast(contract_address as text) as loan_token,
      'Cash' as borrower,
      symbol as token,
      NULL as loan_issued_timestamp,
      NULL as loan_repaid_timestamp,
      NULL as loan_end_timestamp,
      0 as loan_amount,
      0 as projected_interest,
      sum(amount / 10 ^ decimals) as tvl,
      0 as apy,
      'ACTIVE' as status,
      0 as fee_to_stakers,
      0 as establishment_fees
    FROM
      transfers_cash
      LEFT JOIN erc20.tokens erc ON token_address = erc.contract_address
    WHERE
      symbol != ''
    GROUP BY
      symbol,
      pool,
      contract_address
  ),
  merge_dao_lines AS (
    SELECT
      *
    FROM
      final_pool_dao
    UNION all
    SELECT
      *
    FROM
      cash_balance
    WHERE
      token in ('USDC', 'TUSD', 'USDT', 'BUSD')
  ) -- ASSET CREDIT MANAGERS LINES
,
  issuance as (
    SELECT
      bl."instrumentId" as id,
      bl.contract_address as bullet_loan,
      _recipient,
      evt_block_time as loan_issued_date,
      evt_block_time + _duration / 24 / 3600 * INTERVAL '1 DAYS' as loan_term_date,
      _duration / 24 / 3600 as term,
      blc."_underlyingToken" as collat_address,
      _principal / (10 ^ d.decimals) as loan_amount,
      blc."_totalDebt" / (10 ^ d.decimals) as debt,
      (blc."_totalDebt" / _principal - 1) * 365 / (_duration / 24 / 3600) as apy,
      CASE
        WHEN DATE_PART('day', NOW() :: timestamp - evt_block_time :: timestamp) > _duration / 24 / 3600 then 'REPAID'
        else 'ACTIVE'
      end as status
    FROM
      truefi."BulletLoans_evt_LoanCreated" bl
      LEFT JOIN truefi."BulletLoans_call_createLoan" blc on bl."evt_tx_hash" = blc."call_tx_hash"
      LEFT JOIN erc20."tokens" d on d."contract_address" = blc."_underlyingToken"
  ),
  repayment as (
    SELECT
      "instrumentId" as id,
      amount / 1e6 as repaid_amount,
      evt_block_time as loan_repaid_timestamp
    FROM
      truefi."BulletLoans_evt_LoanRepaid"
  ),
  manager as (
    SELECT
      "to" as portfolio,
      "tokenId" as id,
      m."manager" as manager
    FROM
      erc721."ERC721_evt_Transfer" t
      left JOIN truefi."ManagedPortfolioFactory_evt_PortfolioCreated" m on m."newPortfolio" = t."to"
    WHERE
      t.contract_address = '\x8262F360bd5E08a7f4128a1ddBB7D2a17F479239'
  ),
  asset_lines as (
    SELECT
      cast(i.id as text),
      cast(i._recipient as text) as borrower,
      --cast(portfolio as text) as borrower,
      'USDC' as token,
      cast(loan_issued_date as text) as loan_issued_timestamp,
      cast(loan_repaid_timestamp as text),
      cast(loan_term_date as text) as loan_end_timestamp,
      loan_amount,
      CASE
        WHEN status = 'REPAID' then (debt - loan_amount)
        ELSE (debt - loan_amount) * (
          DATE_PART(
            'day',
            NOW() :: timestamp - loan_issued_date :: timestamp
          ) / DATE_PART(
            'day',
            loan_term_date :: timestamp - loan_issued_date :: timestamp
          )
        )
      END AS projected_interest,
      CASE
        WHEN status = 'REPAID' then 0
        ELSE loan_amount * (
          1 + apy * (
            DATE_PART(
              'day',
              NOW() :: timestamp - loan_issued_date :: timestamp
            ) / 365
          )
        )
      END AS tvl,
      apy,
      status,
      0 as fee_to_stakers,
      0 as establishment_fees
    FROM
      issuance i
      left JOIN repayment r on i.id = r.id
      left JOIN manager m on m.id = i.id
  ),
  loans_truefi as (
    SELECT
      *
    FROM
      asset_lines
    UNION ALL
    SELECT
      *
    FROM
      merge_dao_lines
  ),
  truefi_loans as (
    SELECT
      'Truefi' as Protocol,
      CAST(loan_issued_timestamp AS TEXT) as Beginning_Date,
      CAST(loan_end_timestamp AS TEXT) as End_Date,
      CASE
        WHEN borrower = '\x964d9d1a532b5a5daeacbac71d46320de313ae9c' THEN 'Alameda Research'
        WHEN borrower = '\xde797902829fba5d24cdad8dc20ec2329654cf17' THEN 'Delt.ai'
        WHEN borrower = '\x12b5c9191e186658841f2431943c47278f68e075' THEN 'Amber Group'
        WHEN borrower LIKE '\x%' THEN 'Unknown'
        ELSE borrower
      END AS Borrower,
      status as Status,
      loan_amount as Loan_Amount,
      to_char(apy * 100, '99.999%') as APY,
      projected_interest as Interest_To_Lenders,
      establishment_fees + fee_to_stakers as Protocol_Revenues
    FROM
      loans_truefi
  ),
  -- GOLDFINCH PROTOCOL
  e_raw AS (
    -- DEPOSITS
    SELECT
      "evt_block_time" AS ts,
      "owner" AS user,
      'deposit' AS type,
      amount,
      "contract_address" as contract
    FROM
      goldfinch."MigratedTranchedPool_evt_DepositMade" -- WITHDRAWS
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "owner" AS user,
      'withdrawal' AS type,
      "interestWithdrawn" + "principalWithdrawn" AS amount,
      "contract_address" as contract
    FROM
      goldfinch."MigratedTranchedPool_evt_WithdrawalMade" -- DRAWDOWNS
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "borrower" AS user,
      'drawdown' AS type,
      amount,
      "contract_address" as contract
    FROM
      goldfinch."MigratedTranchedPool_evt_DrawdownMade" -- INTEREST
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "payer" AS user,
      'interest' AS type,
      "interestAmount" AS amount,
      "pool" as contract
    FROM
      goldfinch."MigratedTranchedPool_evt_PaymentApplied" -- PRINCIPAL
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "payer" AS user,
      'principal' AS type,
      "principalAmount" AS amount,
      "contract_address" as contract
    FROM
      goldfinch."MigratedTranchedPool_evt_PaymentApplied" -- WRITEDOWN
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "contract_address" AS user,
      'writedown' AS type,
      amount,
      "contract_address" as contract
    FROM
      goldfinch."Pool_evt_PrincipalWrittendown"
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "contract_address" AS user,
      'writedown' AS type,
      amount,
      "contract_address" as contract
    FROM
      goldfinch."SeniorPool_evt_PrincipalWrittenDown" -- ALLOCATION
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "payer" AS user,
      'allocation' AS type,
      "reserveAmount" AS amount,
      "contract_address" as contract
    FROM
      goldfinch."MigratedTranchedPool_evt_PaymentApplied" -- REVENUE
    UNION ALL
    SELECT
      "evt_block_time" AS ts,
      "contract_address" AS user,
      'revenue' AS type,
      amount,
      "contract_address" as contract
    FROM
      goldfinch."MigratedTranchedPool_evt_ReserveFundsCollected"
  ),
  e AS (
    SELECT
      e_raw.user,
      CASE
        WHEN e_raw.type = 'deposit' THEN amount / 1e6
        ELSE 0
      END AS deposit_amt,
      CASE
        WHEN e_raw.type = 'withdrawal' THEN amount / 1e6
        ELSE 0
      END AS withdrawal_amt,
      CASE
        WHEN e_raw.type = 'drawdown' THEN amount / 1e6
        ELSE 0
      END AS drawdown_amt,
      CASE
        WHEN e_raw.type = 'allocation' THEN amount / 1e6
        ELSE 0
      END AS allocation_amt,
      CASE
        WHEN e_raw.type = 'interest' THEN amount / 1e6
        ELSE 0
      END AS interest_amt,
      CASE
        WHEN e_raw.type = 'principal' THEN amount / 1e6
        ELSE 0
      END AS principal_amt,
      CASE
        WHEN e_raw.type = 'writedown' THEN amount / 1e6
        ELSE 0
      END AS writedown_amt,
      CASE
        WHEN e_raw.type = 'revenue' THEN amount / 1e6
        ELSE 0
      END AS revenue_amt,
      contract
    FROM
      e_raw
  ),
  dates AS (
    SELECT
      MIN(block_time) as beginning_date,
      contract_address
    FROM
      ethereum.logs
    WHERE
      contract_address in (
        '\xefeb69edf6b6999b0e3f2fa856a2acf3bdea4ab5',
        '\x418749e294cabce5a714efccc22a8aade6f9db57',
        '\xc9bdd0d3b80cc6efe79a82d850f44ec9b55387ae',
        '\xd09a57127bc40d680be7cb061c2a6629fe71abef',
        '\xf74ea34ac88862b7ff419e60e476be2651433e68',
        '\x00c27fc71b159a346e179b4a1608a0865e8a7470',
        '\x1d596d28a7923a22aa013b0e7082bba23daa656b',
        '\xe32c22e4d95cae1fb805c60c9e0026ed57971bcf',
        '\x759f097f3153f5d62ff1c2d82ba78b6350f223e3',
        '\x89d7c618a4eef3065da8ad684859a547548e6169',
        '\xc13465ce9ae3aa184eb536f04fdc3f54d2def277',
        '\xaa2ccc5547f64c5dffd0a624eb4af2543a67ba65',
        '\xd43a4f3041069c6178b99d55295b00d0db955bb5',
        '\xe6c30756136e07eb5268c3232efbfbe645c1ba5a',
        '\xb26b42dd5771689d0a7faeea32825ff9710b9c11',
        '\x1e73b5c1a3570b362d46ae9bf429b25c05e514a7',
        '\x8bbd80f88e662e56b918c353da635e210ece93c6',
        '\x1cc90f7bb292dab6fa4398f3763681cfe497db97',
        '\x2107ade0e536b8b0b85cca5e0c0c3f66e58c053c',
        '\x3634855ec1beaf6f9be0f7d2f67fc9cb5f4eeea4',
        '\x67df471eacd82c3dbc95604618ff2a1f6b14b8a1',
        '\x9e8b9182abba7b4c188c979bc8f4c79f7f4c90d3',
        '\xd798d527f770ad920bb50680dbc202bb0a1dafd6'
      )
    GROUP BY
      contract_address
  ),
  e_cume AS (
    SELECT
      SUM(deposit_amt) AS deposit_cume,
      SUM(withdrawal_amt) AS withdrawal_cume,
      SUM(drawdown_amt) AS drawdown_cume,
      SUM(allocation_amt) AS allocation_cume,
      SUM(interest_amt) AS interest_cume,
      SUM(principal_amt) AS principal_cume,
      SUM(writedown_amt) AS writedown_cume,
      SUM(interest_amt - writedown_amt) AS net_gain_cume,
      SUM(deposit_amt - withdrawal_amt - allocation_amt) AS net_deposit_cume,
      SUM(revenue_amt) as revenue_cume,
      contract,
      beginning_date,
      CASE
        WHEN contract = '\xefeb69edf6b6999b0e3f2fa856a2acf3bdea4ab5' THEN '2024-03-03'
        WHEN contract = '\x418749e294cabce5a714efccc22a8aade6f9db57' THEN '2025-02-20'
        WHEN contract = '\xc9bdd0d3b80cc6efe79a82d850f44ec9b55387ae' THEN '2024-11-10'
        WHEN contract = '\xd09a57127bc40d680be7cb061c2a6629fe71abef' THEN '2025-02-25'
        WHEN contract = '\xf74ea34ac88862b7ff419e60e476be2651433e68' THEN '2022-03-09'
        WHEN contract = '\x00c27fc71b159a346e179b4a1608a0865e8a7470' THEN '2026-02-21'
        WHEN contract = '\x1d596d28a7923a22aa013b0e7082bba23daa656b' THEN '2024-12-20'
        WHEN contract = '\xe32c22e4d95cae1fb805c60c9e0026ed57971bcf' THEN '2022-02-28'
        WHEN contract = '\x759f097f3153f5d62ff1c2d82ba78b6350f223e3' THEN '2024-04-11'
        WHEN contract = '\x89d7c618a4eef3065da8ad684859a547548e6169' THEN '2025-04-25'
        WHEN contract = '\xc13465ce9ae3aa184eb536f04fdc3f54d2def277' THEN '2022-03-03'
        WHEN contract = '\xaa2ccc5547f64c5dffd0a624eb4af2543a67ba65' THEN '2023-10-22'
        WHEN contract = '\xd43a4f3041069c6178b99d55295b00d0db955bb5' THEN '2025-05-03'
        WHEN contract = '\xe6c30756136e07eb5268c3232efbfbe645c1ba5a' THEN '2024-11-29'
        WHEN contract = '\xb26b42dd5771689d0a7faeea32825ff9710b9c11' THEN '2024-03-04'
        WHEN contract = '\x1e73b5c1a3570b362d46ae9bf429b25c05e514a7' THEN '2021-14-12'
        WHEN contract = '\x8bbd80f88e662e56b918c353da635e210ece93c6' THEN '2022-07-06'
        WHEN contract = '\x1cc90f7bb292dab6fa4398f3763681cfe497db97' THEN '2022-06-27'
        WHEN contract = '\x2107ade0e536b8b0b85cca5e0c0c3f66e58c053c' THEN '2022-02-12'
        WHEN contract = '\x3634855ec1beaf6f9be0f7d2f67fc9cb5f4eeea4' THEN '2022-03-01'
        WHEN contract = '\x67df471eacd82c3dbc95604618ff2a1f6b14b8a1' THEN '2022-03-01'
        WHEN contract = '\x9e8b9182abba7b4c188c979bc8f4c79f7f4c90d3' THEN '2022-04-12'
        WHEN contract = '\xd798d527f770ad920bb50680dbc202bb0a1dafd6' THEN '2022-02-04'
        ELSE 'OTHER'
      END AS end_date,
      CASE
        WHEN contract = '\xefeb69edf6b6999b0e3f2fa856a2acf3bdea4ab5' THEN 0.125
        WHEN contract = '\x418749e294cabce5a714efccc22a8aade6f9db57' THEN 0.1
        WHEN contract = '\xc9bdd0d3b80cc6efe79a82d850f44ec9b55387ae' THEN 0.125
        WHEN contract = '\xd09a57127bc40d680be7cb061c2a6629fe71abef' THEN 0.1
        WHEN contract = '\xf74ea34ac88862b7ff419e60e476be2651433e68' THEN 0.11
        WHEN contract = '\x00c27fc71b159a346e179b4a1608a0865e8a7470' THEN 0.11
        WHEN contract = '\x1d596d28a7923a22aa013b0e7082bba23daa656b' THEN 0.125
        WHEN contract = '\xe32c22e4d95cae1fb805c60c9e0026ed57971bcf' THEN 0.15
        WHEN contract = '\x759f097f3153f5d62ff1c2d82ba78b6350f223e3' THEN 0.1
        WHEN contract = '\x89d7c618a4eef3065da8ad684859a547548e6169' THEN 0.1
        WHEN contract = '\xc13465ce9ae3aa184eb536f04fdc3f54d2def277' THEN 0.1
        WHEN contract = '\xaa2ccc5547f64c5dffd0a624eb4af2543a67ba65' THEN 0.13
        WHEN contract = '\xd43a4f3041069c6178b99d55295b00d0db955bb5' THEN 0.1
        WHEN contract = '\xe6c30756136e07eb5268c3232efbfbe645c1ba5a' THEN 0.125
        WHEN contract = '\xb26b42dd5771689d0a7faeea32825ff9710b9c11' THEN 0.1
        WHEN contract = '\x1e73b5c1a3570b362d46ae9bf429b25c05e514a7' THEN 0.15
        WHEN contract = '\x8bbd80f88e662e56b918c353da635e210ece93c6' THEN 0.12
        WHEN contract = '\x1cc90f7bb292dab6fa4398f3763681cfe497db97' THEN 0.15
        WHEN contract = '\x2107ade0e536b8b0b85cca5e0c0c3f66e58c053c' THEN 0.15
        WHEN contract = '\x3634855ec1beaf6f9be0f7d2f67fc9cb5f4eeea4' THEN 0.12
        WHEN contract = '\x67df471eacd82c3dbc95604618ff2a1f6b14b8a1' THEN 0.15
        WHEN contract = '\x9e8b9182abba7b4c188c979bc8f4c79f7f4c90d3' THEN 0.12
        WHEN contract = '\xd798d527f770ad920bb50680dbc202bb0a1dafd6' THEN 0.15
        ELSE 0
      END AS apr,
      CASE
        WHEN contract = '\xefeb69edf6b6999b0e3f2fa856a2acf3bdea4ab5' THEN 'Almavest Basket #3'
        WHEN contract = '\x418749e294cabce5a714efccc22a8aade6f9db57' THEN 'Almavest Basket #6'
        WHEN contract = '\xc9bdd0d3b80cc6efe79a82d850f44ec9b55387ae' THEN 'Cauris'
        WHEN contract = '\xd09a57127bc40d680be7cb061c2a6629fe71abef' THEN 'Cauris Fund #2: Africa Innovation Pool'
        WHEN contract = '\xf74ea34ac88862b7ff419e60e476be2651433e68' THEN 'Divibank'
        WHEN contract = '\x00c27fc71b159a346e179b4a1608a0865e8a7470' THEN 'Secured U.S. Fintech Yield via Stratos'
        WHEN contract = '\x1d596d28a7923a22aa013b0e7082bba23daa656b' THEN 'Almavest Basket #5'
        WHEN contract = '\xe32c22e4d95cae1fb805c60c9e0026ed57971bcf' THEN 'Almavest Basket #2'
        WHEN contract = '\x759f097f3153f5d62ff1c2d82ba78b6350f223e3' THEN 'Almavest Basket #7: Fintech AND Carbon Reduction Basket'
        WHEN contract = '\x89d7c618a4eef3065da8ad684859a547548e6169' THEN 'Asset-Backed Pool via Addem Capital'
        WHEN contract = '\xc13465ce9ae3aa184eb536f04fdc3f54d2def277' THEN 'Oya, via Almavest'
        WHEN contract = '\xaa2ccc5547f64c5dffd0a624eb4af2543a67ba65' THEN 'Tugende'
        WHEN contract = '\xd43a4f3041069c6178b99d55295b00d0db955bb5' THEN 'Cauris Fund #3: Africa Innovation Pool'
        WHEN contract = '\xe6c30756136e07eb5268c3232efbfbe645c1ba5a' THEN 'Almavest Basket #4'
        WHEN contract = '\xb26b42dd5771689d0a7faeea32825ff9710b9c11' THEN 'Lend East #1: Emerging Asia Fintech Pool'
        WHEN contract = '\x1e73b5c1a3570b362d46ae9bf429b25c05e514a7' THEN 'Payjoy'
        WHEN contract = '\x8bbd80f88e662e56b918c353da635e210ece93c6' THEN 'Aspire #3'
        WHEN contract = '\x1cc90f7bb292dab6fa4398f3763681cfe497db97' THEN 'Quickcheck #3'
        WHEN contract = '\x2107ade0e536b8b0b85cca5e0c0c3f66e58c053c' THEN 'Quickcheck #2'
        WHEN contract = '\x3634855ec1beaf6f9be0f7d2f67fc9cb5f4eeea4' THEN 'Aspire #1'
        WHEN contract = '\x67df471eacd82c3dbc95604618ff2a1f6b14b8a1' THEN 'Almavest Basket #1'
        WHEN contract = '\x9e8b9182abba7b4c188c979bc8f4c79f7f4c90d3' THEN 'Aspire #2'
        WHEN contract = '\xd798d527f770ad920bb50680dbc202bb0a1dafd6' THEN 'Quickcheck #1'
        ELSE 'OTHER'
      END AS contract_name
    FROM
      e
      LEFT JOIN dates on dates.contract_address = e.contract
    GROUP BY
      contract,
      beginning_date
  ),
  final_table AS (
    SELECT
      now() AS date,
      beginning_date,
      end_date,
      contract_name as pool_name,
      CASE
        WHEN interest_cume + principal_cume = 0
        AND net_gain_cume = 0 THEN 'IN CREATION'
        WHEN GREATEST(net_gain_cume + net_deposit_cume, 0) < 0.01
        AND net_gain_cume > 0 THEN 'REPAID'
        ELSE 'ACTIVE'
      END AS status,
      drawdown_cume as drawdown,
      principal_cume as principal,
      interest_cume as interest,
      deposit_cume as deposit,
      withdrawal_cume as withdrawal,
      interest_cume + principal_cume AS repayment,
      drawdown_cume - principal_cume - writedown_cume AS loans_outstANDing,
      GREATEST(net_gain_cume + net_deposit_cume, 0) AS tvl,
      (drawdown_cume - principal_cume - writedown_cume) / (NULLIF(net_gain_cume + net_deposit_cume, 0)) * 100 AS utilitization,
      apr as projected_interest_rate,
      revenue_cume as protocol_revenue,
      COALESCE(
        100 * GREATEST(writedown_cume, 0) / NULLIF(drawdown_cume - principal_cume, 0),
        0
      ) + 0.000000000000001 AS default_rate
    FROM
      e_cume
    WHERE
      contract_name != 'OTHER'
  ),
  loans_goldfinch as (
    SELECT
      beginning_date,
      end_date,
      pool_name,
      status,
      CASE
        WHEN drawdown = 0
        AND status = 'Ended' THEN principal
        ELSE drawdown
      END AS drawdown,
      principal,
      interest,
      CASE
        WHEN deposit = 0
        AND status = 'Ended' THEN principal
        ELSE deposit
      END AS deposit,
      withdrawal,
      CASE
        WHEN drawdown = 0
        AND status = 'Ended' THEN 0
        ELSE loans_outstANDing
      END AS loans_outstANDing,
      tvl,
      utilitization,
      projected_interest_rate,
      default_rate,
      protocol_revenue
    FROM
      final_table
    order by
      beginning_date DESC
  ),
  goldfinch_loans as (
    SELECT
      'Goldfinch' as Protocol,
      CAST(beginning_date AS TEXT) as Beginning_Date,
      CAST(end_date AS TEXT) as End_Date,
      pool_name as Borrower,
      status as Status,
      drawdown as Loan_Amount,
      to_char(projected_interest_rate * 100, '99.999%') as APY,
      interest as Interest_To_Lenders,
      protocol_revenue as Protocol_revenues
    FROM
      loans_goldfinch
  ),
  -- MAPLE PROTOCOL
  pool_liquiditylocker as (
    SELECT
      pool,
      "liquidityLocker"
    FROM
      maple."PoolFactory_evt_PoolCreated"
  ),
  loan_debt_liquidity_pool as (
    SELECT
      distinct l.loan,
      l."debtLocker",
      l.contract_address as "liquidityLocker",
      l.amt as loan_amount,
      p.pool
    FROM
      maple."LiquidityLocker_call_fundLoan" l
      left JOIN pool_liquiditylocker p on (l.contract_address = p."liquidityLocker")
    WHERE
      call_success = 'true'
  ),
  loan_drawdowns as (
    --V1 Loans
    SELECT
      date_trunc('day', evt_block_time) as drawdown_date,
      contract_address as loan,
      sum("drawdownAmount") as loan_drawdown
    FROM
      maple."Loan_evt_Drawdown"
    GROUP BY
      1,
      2
    UNION all
    --V2 Loans
    SELECT
      date_trunc('day', evt_block_time) as drawdown_date,
      contract_address as loan,
      sum("amount_") as loan_drawdown
    FROM
      maple."MapleLoan_evt_FundsDrawnDown"
    GROUP BY
      1,
      2
  ),
  loan_payments as (
    --V1 loans
    SELECT
      date_trunc('day', evt_block_time) as payment_date,
      contract_address as loan,
      sum("principalPaid") as principal_paid,
      sum("interestPaid") as interest_paid
    FROM
      maple."Loan_evt_PaymentMade"
    GROUP BY
      1,
      2
    UNION all
    --v2 loans
    SELECT
      date_trunc('day', evt_block_time) as payment_date,
      contract_address as loan,
      sum("principalPaid_") as principal_paid,
      sum("interestPaid_") as interest_paid
    FROM
      maple."MapleLoan_evt_PaymentMade"
    GROUP BY
      1,
      2 --v2 rehANDled
    UNION all
    SELECT
      date_trunc('day', evt_block_time) as payment_date,
      contract_address as loan,
      sum("principalPaid_") as principal_paid,
      sum("interestPaid_") as interest_paid
    FROM
      maple."MapleLoan_evt_LoanClosed"
    GROUP BY
      1,
      2
  ),
  loan_funding_events as (
    --V1 loans
    SELECT
      contract_address as loan,
      sum("amountFunded") as loan_amount_funded
    FROM
      maple."Loan_evt_LoanFunded"
    GROUP BY
      contract_address
    UNION all
    --v2 loans
    SELECT
      contract_address as loan,
      sum("amount_") as loan_amount_funded
    FROM
      maple."MapleLoan_evt_Funded"
    GROUP BY
      contract_address --v2 loans rehANDled - to be changed in the future
    UNION all
    SELECT
      contract_address as loan,
      sum("amount_") as loan_amount_funded
    FROM
      maple."MapleLoan_evt_FundsRedirected"
    GROUP BY
      contract_address
  ),
  collateral_deposits as (
    SELECT
      "to" as "collateralLocker",
      value / pow(10, t.decimals) as collateral_amount
    FROM
      erc20."ERC20_evt_Transfer" e
      left JOIN erc20."tokens" t on (e.contract_address = t.contract_address)
    WHERE
      "to" in (
        SELECT
          "collateralLocker"
        FROM
          maple."LoanFactory_evt_LoanCreated"
      )
      AND e.contract_address in (
        SELECT
          distinct "collateralAsset"
        FROM
          maple."LoanFactory_evt_LoanCreated"
      )
  ),
  collateralAssets as (
    SELECT
      "collateralAsset"
    FROM
      maple."LoanFactory_evt_LoanCreated"
    UNION all
    SELECT
      assets_[1] as "collateralAsset"
    FROM
      maple."MapleLoan_evt_Initialized"
  ),
  usd_prices as (
    SELECT
      date_trunc('day', minute) as date,
      contract_address as "collateralAsset",
      contract_address as pool_asset,
      round(avg(price) :: numeric, 2) as price_usd
    FROM
      prices."usd" --below is a hack with hard coded contract addresses for USDC, WETH, WBTC.  Gets around very slow query performance FROM commented line (3min runtime)
    WHERE
      contract_address in (
        '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      ) --WHERE contract_address in (SELECT distinct "collateralAsset"  FROM collateralAssets)
      AND minute >= (
        SELECT
          min(call_block_time)
        FROM
          maple."Pool_call_deposit"
      )
    GROUP BY
      1,
      2
  ),
  loans_unwound as (
    SELECT
      contract_address
    FROM
      maple."Loan_call_unwind"
    WHERE
      call_success = 'true'
  ),
  loan_creation_events_multiple as (
    --V1 Loans
    SELECT
      l.evt_block_time,
      date_trunc('day', l.evt_block_time) as loan_start_date,
      l.loan,
      x.pool,
      l.borrower,
      l."liquidityAsset",
      la.symbol as liquidity_asset_symbol,
      l."collateralAsset",
      ca.symbol as collateral_asset_symbol,
      cd.collateral_amount,
      cd.collateral_amount * usd.price_usd as collateral_amount_usd,
      l."collateralLocker",
      l."fundingLocker" --not used anyWHERE
,
      l.specs[1] / 1e4 as loan_apr,
      l.specs[2] as loan_term,
      l.specs[3] as payment_interval --unused
,
      l.specs[4] / pow(10, la.decimals) as loan_amount,
      l.specs[4] / pow(10, la.decimals) * usdl.price_usd as loan_amount_usd,
      l.specs[5] / 1e4 as collat_ratio,
      case
        when lf.loan_amount_funded > l.specs[4] then l.specs[4] / pow(10, la.decimals)
        else coalesce(lf.loan_amount_funded / pow(10, la.decimals), 0)
      end as loan_amount_funded --hack to get around overfunding loans
,
      case
        when lf.loan_amount_funded > l.specs[4] then l.specs[4] / pow(10, la.decimals)
        else coalesce(lf.loan_amount_funded / pow(10, la.decimals), 0) * usdl.price_usd
      end as loan_amount_funded_usd
    FROM
      maple."LoanFactory_evt_LoanCreated" l
      left JOIN erc20."tokens" la on (l."liquidityAsset" = la.contract_address)
      left JOIN erc20."tokens" ca on (l."collateralAsset" = ca.contract_address)
      left JOIN loan_debt_liquidity_pool x on (l.loan = x.loan)
      left JOIN loan_funding_events lf on (l.loan = lf.loan)
      left JOIN collateral_deposits cd on (l."collateralLocker" = cd."collateralLocker")
      left JOIN usd_prices usd on (
        l."collateralAsset" = usd."collateralAsset"
        AND date_trunc('day', l.evt_block_time) = usd."date"
      )
      left JOIN usd_prices usdl on (
        l."liquidityAsset" = usdl."collateralAsset"
        AND date_trunc('day', l.evt_block_time) = usdl."date"
      )
    WHERE
      l.loan not in (
        SELECT
          contract_address
        FROM
          loans_unwound
      )
    UNION all
    --V2 Loans
    SELECT
      l.evt_block_time,
      date_trunc('day', l.evt_block_time) as loan_start_date,
      l.contract_address as loan,
      x.pool,
      l.borrower_ as borrower,
      l.assets_[2] as liquidityAsset,
      la.symbol as liquidity_asset_symbol,
      l.assets_[1] as collateralAsset,
      ca.symbol as collateral_asset_symbol,
      l.amounts_[1] / pow(10, ca.decimals) as collateral_amount,
      l.amounts_[1] / pow(10, ca.decimals) * usd.price_usd as collateral_amount_usd,
      '0x00' as collateralLocker --required in the V1 call to get collateral_amount
      --  , l."fundingLocker" --unused
,
      '0x00' as "fundingLocker",
      l.rates_[1] / 1e18 as loan_apr,
      l."termDetails_" [2] / 60 / 60 / 24 * l."termDetails_" [3] as loan_term --term is paymentInterval(seconds convert to days) * payments
,
      l."termDetails_" [2] / 60 / 60 / 24 as payment_interval --unused
,
      l.amounts_[2] / pow(10, la.decimals) / 1e6 as loan_amount,
      l.amounts_[2] / pow(10, la.decimals) * usdl.price_usd as loan_amount_usd,
      (l.amounts_[1] / pow(10, ca.decimals) * usd.price_usd) / (l.amounts_[2] / pow(10, la.decimals) * usdl.price_usd) as collat_ratio,
      case
        when lf.loan_amount_funded > l.amounts_[2] then l.amounts_[2] / pow(10, la.decimals)
        else coalesce(lf.loan_amount_funded / pow(10, la.decimals), 0)
      end as loan_amount_funded --hack to get around overfunding loans
,
      case
        when lf.loan_amount_funded > l.amounts_[2] then l.amounts_[2] / pow(10, la.decimals)
        else coalesce(lf.loan_amount_funded / pow(10, la.decimals), 0) * usdl.price_usd
      end as loan_amount_funded_usd
    FROM
      maple."MapleLoan_evt_Initialized" l
      left JOIN erc20."tokens" la on (l.assets_[2] = la.contract_address)
      left JOIN erc20."tokens" ca on (l.assets_[1] = ca.contract_address)
      left JOIN loan_debt_liquidity_pool x on (l.contract_address = x.loan)
      left JOIN loan_funding_events lf on (l.contract_address = lf.loan) --  left JOIN collateral_deposits cd on (l."collateralLocker" = cd."collateralLocker")
      left JOIN usd_prices usd on (
        l.assets_[1] = usd."collateralAsset"
        AND date_trunc('day', l.evt_block_time) = usd."date"
      )
      left JOIN usd_prices usdl on (
        l.assets_[2] = usdl."collateralAsset"
        AND date_trunc('day', l.evt_block_time) = usdl."date"
      ) --no equivalent V2 for unwind() known at the moment - may require a WHERE clause here like in the V1 section
    WHERE
      l.contract_address <> '\xc3f20c6eefe39e2be8214dcc8cc41479a115f8ea'
  ) -- remove duplicates
,
  loan_creation_events as (
    SELECT
      distinct *
    FROM
      loan_creation_events_multiple
  ),
  pool_liquidityassets as (
    SELECT
      p.pool,
      p."liquidityAsset",
      t.symbol,
      t.decimals
    FROM
      maple."PoolFactory_evt_PoolCreated" p
      left JOIN erc20."tokens" t on (p."liquidityAsset" = t.contract_address)
  ) --Generate a loan & date cross product
,
  maple_dates as (
    SELECT
      distinct date_trunc('day', "time") as date
    FROM
      ethereum.blocks b
    WHERE
      "time" >= (
        SELECT
          min(call_block_time)
        FROM
          maple."Pool_call_deposit"
      ) --date of first deposit
  ),
  loan_addresses as (
    SELECT
      distinct loan
    FROM
      loan_creation_events
  ),
  date_loan as (
    SELECT
      d.date,
      c.loan
    FROM
      maple_dates d
      cross JOIN loan_addresses c
  ),
  labels as (
    SELECT
      *
    FROM
      (
        values
          (
            '\xfebd6f15df3b73dc4307b1d7e65d46413e710c27' :: bytea,
            'Orthogonal Trading' :: text,
            4 :: integer,
            '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' :: bytea
          ),
          (
            '\x6f6c8013f639979c84b756c7fc1500eb5af18dc4' :: bytea,
            'Maven 11 USDC 01' :: text,
            3 :: integer,
            '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' :: bytea
          ),
          (
            '\xd618d93676762a8e3107554d9adbff7dfd7fbf47' :: bytea,
            'Blocktower Capital' :: text,
            2 :: integer,
            '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' :: bytea
          ),
          (
            '\x3e701d29fcb8747b5c3f88649397d88fff9bd3e9' :: bytea,
            'Alameda Research' :: text,
            1 :: integer,
            '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' :: bytea
          ),
          (
            '\xa1fe1b5fc23c2dab0c28d4cc09021014f30be8f1' :: bytea,
            'Celsius wETH Pool' :: text,
            5 :: integer,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' :: bytea
          ),
          (
            '\x1a066b0109545455bc771e49e6edef6303cb0a93' :: bytea,
            'Maven 11 wETH Pool' :: text,
            6 :: integer,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' :: bytea
          ),
          (
            '\xcc8058526de295c6ad917cb41416366d69a24cde' :: bytea,
            'Maven 11 USDC Permissioned' :: text,
            7 :: integer,
            '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' :: bytea
          )
      ) as pools (pool, pool_name, sort_order, pool_asset)
  ) -- hack here to hANDle the exception with the loan appearing twice (because rehANDled), i changed the last character of the address
,
  loan_funded_amounts as(
    SELECT
      loan_start_date,
      case
        when loan = '\xecfecc0d17b756266ba3b57bcfa8f8be051adf6d'
        AND loan_amount_funded_usd = 5e6 then '\xecfecc0d17b756266ba3b57bcfa8f8be051adf61'
        else loan
      end as loan,
      loan_amount_funded
    FROM
      loan_creation_events
  ),
  date_loan_amt as (
    -- JOIN the loan & pool details into the date_loan table
    --all liquidityAsset units here are still native (their scope is at pool level).  Here we convert collateral units to USD prior to aggregation on pool (their scope is loan)
    SELECT
      dl.date,
      dl.loan,
      l.pool,
      l.loan_term,
      l.loan_start_date,
      lab.pool_name,
      l."liquidityAsset",
      l.liquidity_asset_symbol,
      coalesce(lfa.loan_amount_funded, 0) as loan_amount_funded,
      l.loan_apr,
      l.collat_ratio -- this is the ratio at loan origin
,
      l."collateralAsset",
      l.collateral_asset_symbol,
      l.collateral_amount,
      l.collateral_amount * u.price_usd as collateral_amount_usd -- current value of the collateral, used for dynamic collat_ratio calculation
,
      coalesce(d.loan_drawdown, 0) / pow(10, pla.decimals) as loan_drawdown,
      coalesce(p.principal_paid, 0) / pow(10, pla.decimals) as principal_paid,
      coalesce(p.interest_paid, 0) / pow(10, pla.decimals) as interest_paid
    FROM
      date_loan dl
      left JOIN loan_funded_amounts lfa on (
        dl.loan = lfa.loan
        AND dl.date = lfa.loan_start_date
      )
      left JOIN loan_drawdowns d on (
        dl.loan = d.loan
        AND dl.date = d.drawdown_date
      )
      left JOIN loan_payments p on (
        dl.loan = p.loan
        AND dl.date = p.payment_date
      )
      left JOIN loan_creation_events l on (dl.loan = l.loan) -- AND dl.date = l.loan_start_date)
      left JOIN labels lab on (l.pool = lab.pool)
      left JOIN pool_liquidityassets pla on (l.pool = pla.pool)
      left JOIN usd_prices u on (
        dl.date = u.date
        AND l."collateralAsset" = u."collateralAsset"
      )
    WHERE
      l.pool is not null
  ),
  date_loan_amt_cumu as (
    SELECT
      date,
      loan,
      pool,
      loan_term,
      loan_start_date,
      "liquidityAsset",
      liquidity_asset_symbol,
      "collateralAsset",
      collateral_asset_symbol,
      pool_name,
      sum(loan_amount_funded) over (
        partition by loan
        order by
          date rows between unbounded preceding
          AND current row
      ) as cumu_loan_amount_funded,
      loan_apr,
      collat_ratio,
      case
        when (
          sum(loan_drawdown - principal_paid) over (
            partition by loan
            order by
              date rows between unbounded preceding
              AND current row
          )
        ) > 0 then collateral_amount_usd
        else 0
      end as collateral_amount_usd_cumu,
      collateral_amount_usd,
      sum(loan_drawdown) over (
        partition by loan
        order by
          date rows between unbounded preceding
          AND current row
      ) as cumu_loan_drawdown,
      sum(principal_paid) over (
        partition by loan
        order by
          date rows between unbounded preceding
          AND current row
      ) as cumu_principal_paid --  , sum(loan_drawdown-principal_paid) over (partition by loan order by date rows between unbounded preceding AND current row) as loan_outstANDing --changed FROM loan_drawdown
,
      sum(loan_amount_funded - principal_paid) over (
        partition by loan
        order by
          date rows between unbounded preceding
          AND current row
      ) as loan_outstANDing --changed FROM loan_drawdown
,
      sum(interest_paid) over (
        partition by loan
        order by
          date rows between unbounded preceding
          AND current row
      ) as cumu_interest_paid
    FROM
      date_loan_amt
  ),
  raw as (
    SELECT
      *,
      case
        when cumu_loan_amount_funded < 1000
        AND "liquidityAsset" != '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        else "liquidityAsset"
      end as "liquidityAsset_true",
      case
        when cumu_loan_amount_funded < 1000
        AND "liquidityAsset" != '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then 'WETH'
        else "liquidity_asset_symbol"
      end as liquidity_asset_symbol_true
    FROM
      date_loan_amt_cumu
    WHERE
      date = (
        SELECT
          MAX(date)
        FROM
          date_loan_amt_cumu
      )
  ) --everything is still in native units - convert to USD
,
  summary as (
    SELECT
      l.date,
      l.loan,
      l.pool,
      l.pool_name,
      l.loan_start_date,
      l.loan_term,
      l."liquidityAsset_true",
      l.liquidity_asset_symbol_true,
      l.cumu_loan_amount_funded * u.price_usd as cumu_loan_amount_funded,
      l.cumu_principal_paid * u.price_usd as cumu_principal_paid,
      l.cumu_interest_paid * u.price_usd as cumu_interest_paid,
      l.loan_outstANDing * u.price_usd as loan_outstANDing,
      l.loan_apr,
      l.collat_ratio,
      l.collateral_amount_usd
    FROM
      raw l
      left JOIN usd_prices u on (
        l.date = u.date
        AND l."liquidityAsset_true" = u."collateralAsset"
      )
      left JOIN labels lab on (l.pool = lab.pool)
  ) -- default events
,
  defaults as (
    SELECT
      loan,
      "defaultSuffered" as "amount",
      evt_block_time
    FROM
      maple."Pool_evt_DefaultSuffered"
  ),
  loans_maple as (
    SELECT
      summary.loan,
      pool_name,
      loan_start_date,
      loan_start_date + loan_term * INTERVAL '1 day' as "end_date",
      cumu_loan_amount_funded as loan_drawdown,
      cumu_principal_paid as principal,
      cumu_interest_paid as interest,
      loan_outstANDing,
      loan_apr,
      collat_ratio,
      case
        when cumu_loan_amount_funded > 0 then 1
        else 0
      end as "funded",
      case
        when loan_outstANDing > 0 then 1
        else 0
      end as "outstANDing",
      case
        when cumu_loan_amount_funded = cumu_principal_paid then 1
        else 0
      end as "REPAID",
      case
        when d.amount > 0 then 'RESTRUCTURED/DEFAULTED'
        when cumu_loan_amount_funded > 0
        AND loan_outstANDing > 0
        AND cumu_loan_amount_funded > cumu_principal_paid then 'ACTIVE'
        when cumu_loan_amount_funded > 0
        AND loan_outstANDing = 0
        AND cumu_loan_amount_funded = cumu_principal_paid then 'REPAID'
        else 'TBD'
      end as "status",
      case
        when cumu_loan_amount_funded > 0
        AND loan_outstANDing > 0
        AND cumu_loan_amount_funded > cumu_principal_paid then loan_outstANDing + cumu_interest_paid
        else 0
      end as tvl,
      cumu_loan_amount_funded * 0.0066 * loan_term / 365 as protocol_fee,
      cumu_loan_amount_funded * 0.0033 * loan_term / 365 as delegator_fee
    FROM
      summary
      left JOIN defaults d on d.loan = summary.loan
  ),
  maple_loans_woborrower as (
    SELECT
      'Maple' as Protocol,
      CAST(loan_start_date as TEXT) as Beginning_Date,
      CAST(end_date as TEXT) as End_Date,
      status as Status,
      loan_drawdown as Loan_Amount,
      to_char(loan_apr * 100, '99.99%') as APY,
      interest as Interest_To_Lenders,
      protocol_fee as Protocol_Revenues
    FROM
      loans_maple
  ),
  maple_loans as (
    SELECT
      protocol,
      Beginning_Date,
      End_Date,
      CASE
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-15_2000000_11.00%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-15_1500000_11.50%' THEN 'Apollo Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-18_4000000_9.00%' THEN 'Symbolic Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-19_250000_22.00%' THEN 'Gattaca'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-30_5000000_9.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-03_5000000_9.00%' THEN 'Parallel Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-08_2500000_9.50%' THEN 'OVEX'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-20_5000000_8.38%' THEN 'Parallel Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-22_4650000_9.00%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-26_7500000_8.00%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-03_8500000_9.50%' THEN 'mgnr.io'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-03_10000000_7.75%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-10_8700000_10.00%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-29_4000000_10.25%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-11_3000000_11.25%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-13_5000000_9.50%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-14_6000000_11.25%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-14_3000000_11.00%' THEN 'Apollo Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-20_12000000_9.75%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-26_2500000_10.90%' THEN 'Parallel Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-28_2000000_12.50%' THEN 'Subspace Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-28_5000000_10.75%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-06_3000000_12.00%' THEN 'OVEX'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-09_5000000_12.00%' THEN 'Fasanara Investments'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-16_2000000_12.00%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-20_7000000_12.25%' THEN 'Wincent'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-30_5000000_9.25%' THEN 'Fasanara Investments'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-30_6000000_8.50%' THEN 'GSR Markets Ltd'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-02_2500000_10.50%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-03_5000000_11.25%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-04_7000000_10.75%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-10_8000000_8.00%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-10_9000000_11.00%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-14_3000000_10.50%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-12_4000000_8.64%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-15_4000000_7.75%' THEN 'Symbolic Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-25_5000000_10.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-31_2000000_9.00%' THEN 'Parallel Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-03_5000000_9.75%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-07_3000000_8.64%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-08_3000000_10.00%' THEN 'Apollo Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-09_6000000_10.25%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-10_5000000_7.40%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-17_4650000_8.75%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-23_5500000_9.00%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-23_6000000_9.75%' THEN 'Wincent'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-24_10000000_7.60%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-25_20000000_7.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-01_10000000_7.60%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-01_3000000_9.25%' THEN 'Dexterity Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-02_14000000_9.75%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-11_16000000_8.50%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-13_10000000_8.00%' THEN 'Auros'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-14_24000000_8.00%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-24_13000000_8.75%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-27_17000000_8.75%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-30_5000000_8.75%' THEN 'Dexterity Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-08_10000000_9.75%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-08_10000000_9.00%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-11_5000000_9.50%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-13_7000000_8.00%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-15_3000000_8.00%' THEN 'Dexterity Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-15_2500000_11.00%' THEN 'Edge DeFi Master Fund LP'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-05-05_10000000_7.50%' THEN 'Auros'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-05-26_7500000_9.50%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-05-27_12000000_7.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-06-02_6000000_8.65%' THEN 'Wincent'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-06-05_17000000_7.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-06-07_9000000_8.80%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-11_3675625_5.00%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-11_4043187_4.25%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-11_2352400_6.00%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-13_3528600_4.80%' THEN 'Auros'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-24_2108338_5.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-08_1764300_5.00%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-11_4153456_4.75%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-06-01_2205375_5.00%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-06-04_3528600_4.00%' THEN 'Auros'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-15_2940500_5.00%' THEN 'Auros'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-16_4410750_7.50%' THEN 'Symbolic Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-16_13232249_5.00%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-22_2940500_7.50%' THEN 'Orthogonal Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-12_16000000_10.75%' THEN 'Flow Traders'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-12_13200000_11.50%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-15_7500000_13.00%' THEN 'Auros'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_2000000_10.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_2000000_10.00%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_2000000_15.00%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_2000000_15.00%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_1000000_12.00%' THEN 'Invictus Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_2000000_15.00%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_2000000_15.00%' THEN 'Symbolic Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-26_2000000_15.00%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-05-27_2000000_15.00%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-02_4000000_11.00%' THEN 'mgnr.io'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-02_4000000_9.00%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-15_4000000_8.50%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-07-21_4000000_9.00%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-10_4000000_9.00%' THEN 'Vexil Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-12_5000000_8.00%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-12_5000000_8.00%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-15_5800000_7.75%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-19_5000000_8.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-20_10000000_8.00%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-20_4000000_8.00%' THEN 'Symbolic Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-24_8000000_7.50%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-08-26_9500000_7.75%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-06_15000000_8.38%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-07_10000000_8.25%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-14_1000000_8.50%' THEN 'JST Digital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-09-28_3000000_10.75%' THEN 'Nascent'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-02_5000000_10.50%' THEN 'Vexil Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-10_11200000_11.00%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-12_1000000_9.75%' THEN 'Akuna'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-15_5000000_11.00%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-10-21_8500000_11.50%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-08_4000000_11.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-12_2000000_11.25%' THEN 'Parallel Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-19_7500000_12.00%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-24_10000000_12.25%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-01_16000000_11.25%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-06_100_8.50%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-08_10000000_11.00%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-08_8000000_11.50%' THEN 'AP Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-13_27500000_10.25%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-23_5000000_11.00%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-18_5000000_9.75%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-24_7500000_10.75%' THEN 'Nascent'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-05_5000000_9.00%' THEN 'Vexil Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-07_10000000_8.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-09_12000000_8.75%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-11_3000000_9.00%' THEN 'Parallel Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-13_5000000_10.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-15_15000000_8.75%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-15_10000000_9.75%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-15_7500000_7.75%' THEN 'Symbolic Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-20_32000000_7.60%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-20_6000000_9.75%' THEN 'Wincent'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-25_17000000_8.75%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-09_17000000_7.87%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-18_15000000_8.25%' THEN 'Folkvang'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-22_1000000_9.00%' THEN 'Digital Asset Capital Management'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-23_3000000_8.00%' THEN 'Parallel Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-24_6000000_9.50%' THEN 'Nascent'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-25_9000000_8.50%' THEN 'Reliz Ltd'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-29_20000000_8.50%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-13_10000000_7.25%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-16_10000000_8.20%' THEN 'FBG Capital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-19_2000000_8.00%' THEN 'JST Digital'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-22_18000000_6.50%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-29_7000000_7.25%' THEN 'Bastion Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-05-05_4000000_9.50%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-05-06_10000000_7.60%' THEN 'Babel Block Limited'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-06-10_12000000_7.50%' THEN 'Nibbio'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-24_10000000_11.00%' THEN 'Reliz Ltd'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-11-19_21250000_8.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2021-12-22_23300000_8.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-03_10100000_8.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-01-31_19423790_8.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-18_47250000_7.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-22_77770000_7.25%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-05-05_54030000_6.50%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-05-21_35137000_7.00%' THEN 'Alameda Research'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-24_5486973_4.00%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-24_2743486_4.00%' THEN 'Framework Labs'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-02-24_5486973_3.50%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-03-08_2881690_3.50%' THEN 'Auros'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-04-28_5153226_3.75%' THEN 'Amber Group'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-09-09_40000000_10.75%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-08-16_12226050_5.00%' THEN 'Wintermute Trading'
        WHEN CONCAT(
          DATE(CAST(beginning_date as DATE)),
          CONCAT('_', CONCAT(loan_amount, CONCAT('_', TRIM(apy))))
        ) = '2022-09-08_10000000_10.00%' THEN 'Wintermute Trading'
        ELSE 'Unknown'
      END as Borrower,
      Status,
      Loan_Amount,
      APY,
      Interest_To_Lenders,
      Protocol_Revenues
    FROM
      maple_loans_woborrower
  ),
  --RIBBON PROTOCOL
  e_raw_ribbon AS (
    -- DEPOSITS
    SELECT
      evt_block_time as timeframe,
      "currencyAmount" AS amount,
      'deposit' AS type,
      contract_address as pool
    FROM
      ribbon_lend."PoolMaster_evt_Provided" -- WITHDRAWS
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      "currencyAmount" AS amount,
      'withdraw' AS type,
      contract_address as pool
    FROM
      ribbon_lend."PoolMaster_evt_Redeemed" -- DRAWDOWNS
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      "amount" AS amount,
      'drawdown' AS type,
      contract_address as pool
    FROM
      ribbon_lend."PoolMaster_evt_Borrowed" -- PRINCIPAL AND INTERESTS
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      "amount" AS amount,
      'principal' AS type,
      contract_address as pool
    FROM
      ribbon_lend."PoolMaster_evt_Repaid"
  ),
  e_ribbon AS (
    SELECT
      CASE
        WHEN e_raw_ribbon.type = 'deposit' THEN amount / 1e6
        ELSE 0
      END AS deposit_amt,
      CASE
        WHEN e_raw_ribbon.type = 'withdraw' THEN amount / 1e6
        ELSE 0
      END AS withdrawal_amt,
      CASE
        WHEN e_raw_ribbon.type = 'drawdown' THEN amount / 1e6
        ELSE 0
      END AS drawdown_amt,
      CASE
        WHEN e_raw_ribbon.type = 'principal' THEN amount / 1e6
        ELSE 0
      END AS principal_amt,
      pool
    FROM
      e_raw_ribbon
  ),
  dates_ribbon AS (
    SELECT
      MIN(block_time) as beginning_date,
      contract_address
    FROM
      ethereum.logs
    WHERE
      contract_address in (
        '\x0Aea75705Be8281f4c24c3E954D1F8b1D0f8044C',
        '\x3CD0ecf1552D135b8Da61c7f44cEFE93485c616d'
      )
    GROUP BY
      contract_address
  ),
  last_table as (
    SELECT
      beginning_date,
      CASE
        WHEN pool = '\x0Aea75705Be8281f4c24c3E954D1F8b1D0f8044C' THEN '2022-09-24 13:06'
        WHEN pool = '\x3CD0ecf1552D135b8Da61c7f44cEFE93485c616d' THEN '2022-09-24 13:06'
        ELSE 'UNKNOWN'
      END AS end_date,
      CASE
        WHEN pool = '\x0Aea75705Be8281f4c24c3E954D1F8b1D0f8044C' THEN 'ACTIVE'
        WHEN pool = '\x3CD0ecf1552D135b8Da61c7f44cEFE93485c616d' THEN 'ACTIVE'
        ELSE 'UNKNOWN'
      END AS Status,
      CASE
        WHEN pool = '\x0Aea75705Be8281f4c24c3E954D1F8b1D0f8044C' THEN 'Wintermute'
        WHEN pool = '\x3CD0ecf1552D135b8Da61c7f44cEFE93485c616d' THEN 'Folkvang'
        ELSE 'UNKNOWN'
      END AS contract_name,
      SUM(drawdown_amt) AS drawdown,
      'N/A' as principal,
      CASE
        WHEN SUM(drawdown_amt) - SUM(principal_amt) < 0 THEN - (SUM(drawdown_amt) - SUM(principal_amt))
        ELSE 0
      end as interest,
      SUM(deposit_amt) AS deposit,
      SUM(withdrawal_amt) AS withdrawal,
      SUM(principal_amt) AS repayment,
      greatest(SUM(drawdown_amt) - SUM(principal_amt), 0) as outstANDing_loans,
      greatest(SUM(deposit_amt) - SUM(withdrawal_amt), 0) as tvl,
      greatest(
        (SUM(drawdown_amt) - SUM(principal_amt)) / greatest((SUM(deposit_amt) - SUM(withdrawal_amt)), 0.00001),
        0
      ) as utilitization
    FROM
      e_ribbon
      LEFT JOIN dates_ribbon on dates_ribbon.contract_address = e_ribbon.pool
    GROUP BY
      pool,
      beginning_date
  ),
  loans_ribbon as (
    SELECT
      *,
      case
        when utilitization > 0.845714 THEN 3600 * 24 * 365.25 * utilitization * 0.5 * (
          (7081853543 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417)) + (2933155758 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417))
        ) / 2
        when utilitization < 0.845714
        AND utilitization > 0 then 3600 * 24 * 365.25 * utilitization * 0.5 * (
          (4552620134 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417)) + (2933155758 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417))
        ) / 2
        else 3600 * 24 * 365.25 * utilitization * 0.5 * ((2933155758 / 1e18) * (1 + 1)) / 2
      end as apr,
      'N/A' as default_rate
    FROM
      last_table
    WHERE
      drawdown > 0
    order by
      beginning_date DESC
  ),
  ribbon_loans as (
    SELECT
      'Ribbon' as Protocol,
      CAST(beginning_date AS TEXT) as Beginning_Date,
      CAST(end_date AS TEXT) as End_Date,
      contract_name as Borrower,
      status as Status,
      drawdown as Loan_Amount,
      to_char(apr * 100, '99.999%') as APY,
      interest as Interest_To_Lenders,
      0 as Protocol_Revenues
    FROM
      loans_ribbon
  ) -- CLEARPOOL PROTOCOL
,
  e_raw_clearpool AS (
    -- DEPOSITS
    SELECT
      evt_block_time as timeframe,
      "currencyAmount" AS amount,
      'deposit' AS type,
      contract_address as pool
    FROM
      clearpool."PoolMasterV2_evt_Provided" -- permissioned pool deposit
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      value as amount,
      'deposit' AS type,
      '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5' as pool
    FROM
      erc20."ERC20_evt_Transfer"
    WHERE
      "contract_address" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      AND "from" in ('\x395dc7674b4a841acafa3000dfd3b295dad5430c')
      AND evt_block_time in (
        SELECT
          distinct evt_block_time
        FROM
          ethereum.logs
        WHERE
          contract_address = '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5'
      ) -- WITHDRAWS
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      "currencyAmount" AS amount,
      'withdraw' AS type,
      contract_address as pool
    FROM
      clearpool."PoolMasterV2_evt_Redeemed" -- permission pool withdraw
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      value as amount,
      'withdraw' AS type,
      '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5' as pool
    FROM
      erc20."ERC20_evt_Transfer"
    WHERE
      "contract_address" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      AND "from" in ('\x25ce419fa61f901a66e462488000241e1a56858d')
      AND evt_block_time in (
        SELECT
          distinct evt_block_time
        FROM
          ethereum.logs
        WHERE
          contract_address = '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5'
      ) -- DRAWDOWNS
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      "amount" AS amount,
      'drawdown' AS type,
      contract_address as pool
    FROM
      clearpool."PoolMasterV2_evt_Borrowed" -- permissioned pool drawdown
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      value as amount,
      'drawdown' AS type,
      '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5' as pool
    FROM
      erc20."ERC20_evt_Transfer"
    WHERE
      "contract_address" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      AND "from" in ('\x395dc7674b4a841acafa3000dfd3b295dad5430c')
      AND evt_block_time in (
        SELECT
          distinct evt_block_time
        FROM
          ethereum.logs
        WHERE
          contract_address = '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5'
      ) -- PRINCIPAL AND INTERESTS
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      "amount" AS amount,
      'principal' AS type,
      contract_address as pool
    FROM
      clearpool."PoolMasterV2_evt_Repaid" -- permission pool repayment
    UNION ALL
    SELECT
      evt_block_time as timeframe,
      value as amount,
      'principal' AS type,
      '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5' as pool
    FROM
      erc20."ERC20_evt_Transfer"
    WHERE
      "contract_address" = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      AND "from" in ('\x25ce419fa61f901a66e462488000241e1a56858d')
      AND evt_block_time in (
        SELECT
          distinct evt_block_time
        FROM
          ethereum.logs
        WHERE
          contract_address = '\x9eb1c079f0d9b14a5a73c1c3c2b671106ebaa5e5'
      )
  ),
  e_clearpool AS (
    SELECT
      CASE
        WHEN e_raw_clearpool.type = 'deposit' THEN amount / 1e6
        ELSE 0
      END AS deposit_amt,
      CASE
        WHEN e_raw_clearpool.type = 'withdraw' THEN amount / 1e6
        ELSE 0
      END AS withdrawal_amt,
      CASE
        WHEN e_raw_clearpool.type = 'drawdown' THEN amount / 1e6
        ELSE 0
      END AS drawdown_amt,
      CASE
        WHEN e_raw_clearpool.type = 'principal' THEN amount / 1e6
        ELSE 0
      END AS principal_amt,
      pool
    FROM
      e_raw_clearpool
  ),
  dates_clearpool AS (
    SELECT
      MIN(block_time) as beginning_date,
      contract_address
    FROM
      ethereum.logs
    WHERE
      contract_address in (
        '\xCb288b6d30738db7E3998159d192615769794B5b',
        '\xe3D20A721522874D32548B4097d1afc6f024e45b',
        '\x3aeB3a8F0851249682A6a836525CDEeE5aA2A153',
        '\x7B7E4ab1eCf7A69C1F15B186f9AF11Bb53e764Cb',
        '\xBC105b92F6350E02F230e58Cd4240eA76D7d1DCA',
        '\x9Eb1C079F0D9B14a5a73c1c3C2b671106EBAa5E5',
        '\xc5d55dca587d26cdbf1841d72b61f7f74241960c',
        '\x2463a26fe60217174f871f093575a9bcade03f60',
        '\x74fd948187E8503eBa08fD6CF9D23124C55809EB'
      )
    GROUP BY
      contract_address
  ),
  last_table_clearpool as (
    SELECT
      beginning_date,
      CASE
        WHEN pool = '\xCb288b6d30738db7E3998159d192615769794B5b' THEN 'N/A'
        WHEN pool = '\xe3D20A721522874D32548B4097d1afc6f024e45b' THEN 'N/A'
        WHEN pool = '\x3aeB3a8F0851249682A6a836525CDEeE5aA2A153' THEN 'N/A'
        WHEN pool = '\x7B7E4ab1eCf7A69C1F15B186f9AF11Bb53e764Cb' THEN 'N/A'
        WHEN pool = '\xBC105b92F6350E02F230e58Cd4240eA76D7d1DCA' THEN '2022-07-08 13:06'
        WHEN pool = '\x2463a26fe60217174f871f093575a9bcade03f60' THEN '2022-06-15 12:00'
        WHEN pool = '\xc5d55dca587d26cdbf1841d72b61f7f74241960c' THEN '2022-05-28 12:00'
        WHEN pool = '\x74fd948187E8503eBa08fD6CF9D23124C55809EB' THEN '2022-09-22 14:00'
        ELSE 'UNKNOWN'
      END AS end_date,
      CASE
        WHEN pool = '\xCb288b6d30738db7E3998159d192615769794B5b' THEN 'ACTIVE'
        WHEN pool = '\xe3D20A721522874D32548B4097d1afc6f024e45b' THEN 'ACTIVE'
        WHEN pool = '\x3aeB3a8F0851249682A6a836525CDEeE5aA2A153' THEN 'ACTIVE'
        WHEN pool = '\x7B7E4ab1eCf7A69C1F15B186f9AF11Bb53e764Cb' THEN 'ACTIVE'
        WHEN pool = '\xBC105b92F6350E02F230e58Cd4240eA76D7d1DCA' THEN 'REPAID'
        WHEN pool = '\x2463a26fe60217174f871f093575a9bcade03f60' THEN 'REPAID'
        WHEN pool = '\xc5d55dca587d26cdbf1841d72b61f7f74241960c' THEN 'REPAID'
        WHEN pool = '\x74fd948187E8503eBa08fD6CF9D23124C55809EB' THEN 'ACTIVE'
        ELSE 'UNKNOWN'
      END AS Status,
      CASE
        WHEN pool = '\xCb288b6d30738db7E3998159d192615769794B5b' THEN 'Wintermute Trading 2'
        WHEN pool = '\xe3D20A721522874D32548B4097d1afc6f024e45b' THEN 'Folkvang'
        WHEN pool = '\x3aeB3a8F0851249682A6a836525CDEeE5aA2A153' THEN 'Auros'
        WHEN pool = '\x7B7E4ab1eCf7A69C1F15B186f9AF11Bb53e764Cb' THEN 'Amber Group'
        WHEN pool = '\xBC105b92F6350E02F230e58Cd4240eA76D7d1DCA' THEN 'FBG Capital'
        WHEN pool = '\xc5d55dca587d26cdbf1841d72b61f7f74241960c' THEN 'Wintermute Trading 1'
        WHEN pool = '\x9Eb1C079F0D9B14a5a73c1c3C2b671106EBAa5E5' THEN 'Jane Street'
        WHEN pool = '\x2463a26fe60217174f871f093575a9bcade03f60' THEN 'TPS Capital'
        WHEN pool = '\x74fd948187E8503eBa08fD6CF9D23124C55809EB' THEN 'Nibbio'
        ELSE 'UNKNOWN'
      END AS contract_name,
      SUM(drawdown_amt) AS drawdown,
      'N/A' as principal,
      CASE
        WHEN SUM(drawdown_amt) - SUM(principal_amt) < 0 THEN - (SUM(drawdown_amt) - SUM(principal_amt))
        ELSE 0
      end as interest,
      SUM(deposit_amt) AS deposit,
      SUM(withdrawal_amt) AS withdrawal,
      SUM(principal_amt) AS repayment,
      greatest(SUM(drawdown_amt) - SUM(principal_amt), 0) as outstANDing_loans,
      greatest(SUM(deposit_amt) - SUM(withdrawal_amt), 0) as tvl,
      greatest(
        (SUM(drawdown_amt) - SUM(principal_amt)) / greatest((SUM(deposit_amt) - SUM(withdrawal_amt)), 0.00001),
        0
      ) as utilitization
    FROM
      e_clearpool
      LEFT JOIN dates_clearpool on dates_clearpool.contract_address = e_clearpool.pool
    GROUP BY
      pool,
      beginning_date
  ),
  loans_clearpool as (
    SELECT
      *,
      case
        when utilitization > 0.845714 THEN 3600 * 24 * 365.25 * utilitization * 0.5 * (
          (7081853543 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417)) + (2933155758 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417))
        ) / 2
        when utilitization < 0.845714
        AND utilitization > 0 then 3600 * 24 * 365.25 * utilitization * 0.5 * (
          (4552620134 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417)) + (2933155758 / 1e18) * (1 + COS(2 * 3.1415) * utilitization ^ (-1 / -0.2417))
        ) / 2
        else 3600 * 24 * 365.25 * utilitization * 0.5 * ((2933155758 / 1e18) * (1 + 1)) / 2
      end as apr,
      'N/A' as default_rate
    FROM
      last_table_clearpool
    WHERE
      drawdown > 0
    order by
      beginning_date DESC
  ),
  clearpool_loans as (
    SELECT
      'Clearpool' as Protocol,
      CAST(beginning_date AS TEXT) as Beginning_Date,
      CAST(end_date AS TEXT) as End_Date,
      contract_name as Borrower,
      status as Status,
      drawdown as Loan_Amount,
      to_char(apr * 100, '99.999%') as APY,
      interest as Interest_To_Lenders,
      0 as Protocol_Revenues
    FROM
      loans_clearpool
  ),
  -- CENTRIFUGE
  e_raw_centrifuge as (
    -- DRAWDOWN
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'New Silver' as pool,
      'drawdown' as type
    FROM
      centrifuge."new_silver_2_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Bling' as pool,
      'drawdown' as type
    FROM
      centrifuge."bling_series_1_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Cauris' as pool,
      'drawdown' as type
    FROM
      centrifuge."CaurisShelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Consol freight' as pool,
      'drawdown' as type
    FROM
      centrifuge."consolfreight_4_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Databased' as pool,
      'drawdown' as type
    FROM
      centrifuge."databased_1_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Flowcarbon' as pool,
      'drawdown' as type
    FROM
      centrifuge."flowcarbon_1_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'FortunaFi' as pool,
      'drawdown' as type
    FROM
      centrifuge."fortunafi_1_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Branch' as pool,
      'drawdown' as type
    FROM
      centrifuge."branch_3_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'GigPool' as pool,
      'drawdown' as type
    FROM
      centrifuge."GigpoolShelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Harbor Trade' as pool,
      'drawdown' as type
    FROM
      centrifuge."harbor_trade_2_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Paperchain' as pool,
      'drawdown' as type
    FROM
      centrifuge."paperchain_3_shelf_call_borrow" -- WHERE call_success = 'true'
    UNION all
    SELECT
      "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'SPV' as pool,
      'drawdown' as type
    FROM
      centrifuge."SPVShelf_call_borrow" -- WHERE call_success = 'true'
      --REPAYMENT
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'New Silver' as pool,
      'principal' as type
    FROM
      centrifuge."new_silver_2_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Bling' as pool,
      'principal' as type
    FROM
      centrifuge."bling_series_1_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Cauris' as pool,
      'principal' as type
    FROM
      centrifuge."CaurisShelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Consol freight' as pool,
      'principal' as type
    FROM
      centrifuge."consolfreight_4_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Databased' as pool,
      'principal' as type
    FROM
      centrifuge."databased_1_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Flowcarbon' as pool,
      'principal' as type
    FROM
      centrifuge."flowcarbon_1_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'FortunaFi' as pool,
      'principal' as type
    FROM
      centrifuge."fortunafi_1_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Branch' as pool,
      'principal' as type
    FROM
      centrifuge."branch_3_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'GigPool' as pool,
      'principal' as type
    FROM
      centrifuge."GigpoolShelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Harbor Trade' as pool,
      'principal' as type
    FROM
      centrifuge."harbor_trade_2_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'Paperchain' as pool,
      'principal' as type
    FROM
      centrifuge."paperchain_3_shelf_call_repay" -- WHERE call_success = 'true'
    UNION all
    SELECT
      - "currencyAmount" / 1e18 as amount,
      date_trunc('day', "call_block_time") as time,
      'SPV' as pool,
      'principal' as type
    FROM
      centrifuge."SPVShelf_call_repay" -- WHERE call_success = 'true'
  ),
  centrifuge_time as (
    SELECT
      pool,
      min(time) as time
    FROM
      e_raw_centrifuge
    GROUP BY
      pool
  ),
  loans_centrifuge as (
    SELECT
      pool,
      sum(amount) as amount
    FROM
      e_raw_centrifuge
    GROUP BY
      pool
  ),
  centrifuge_loans as (
    SELECT
      'Centrifuge' as Protocol,
      CAST(time AS TEXT) as Beginning_Date,
      'N/A' as End_Date,
      loans_centrifuge.pool as Borrower,
      'Active' as Status,
      amount as Loan_Amount,
      'N/A' as APY,
      0 as Interest_To_Lenders,
      0 as Fee_To_Stakers
    FROM
      loans_centrifuge
      inner JOIN centrifuge_time on centrifuge_time.pool = loans_centrifuge.pool -- inner JOIN centrifuge_apy on centrifuge_apy.pool = loans_centrifuge.pool
  ),
  -- ALL PROTOCOL
  all_protocols_loans as (
    SELECT
      *
    FROM
      truefi_loans
    UNION ALL
    SELECT
      *
    FROM
      goldfinch_loans
    UNION ALL
    SELECT
      *
    FROM
      maple_loans
    UNION ALL
    SELECT
      *
    FROM
      clearpool_loans
    UNION ALL
    SELECT
      *
    FROM
      ribbon_loans
    UNION ALL
    SELECT
      *
    FROM
      centrifuge_loans
  ) -- FINAL QUERY
SELECT
  *
FROM
  all_protocols_loans
WHERE
  (
    protocol = '{{protocol}}'
    or '{{protocol}}' = 'All Protocols'
  )
  AND (
    Status = '{{status}}'
    or '{{status}}' = 'All Status'
  )
  AND Loan_Amount > 0
ORDER BY
  Beginning_Date DESC