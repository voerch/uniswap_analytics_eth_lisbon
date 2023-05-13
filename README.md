# ETH Lisbon Uniswap Submission
Mustafa Berke Erdis - Polya AI


What: Concentrated Liquidity can be frustrating to many, our tool enables LPs to determine which ticks are worth concentrating on.

How: By design every swap event contains ex-ante active liquidity in the current tick range. We parsed these on-chain swap events, calculated fee accrued from each swap to the current providers and how much liquidity was active in during the swap. Hence creating a new metric named highest yield per dollar.

Further Improvements: Due to computational costs only select number of pools were analyzed, this can be expanded into any existing pool. Additionally, many rebalancing strategies can be built on this simple idea. For example, just like a momentum strategy one may choose to quote a wide range of ticks in case of trending yield per dollar & price. 
