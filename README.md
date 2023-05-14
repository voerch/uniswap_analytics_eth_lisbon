# ETH Global Lisbon
Concentrated Liquidity can be frustrating to many, our tool enables LPs to determine which ticks are worth concentrating on.

The concept of concentrated liquidity enables well-informed market participants to quote on better ticks hence earning more fees. 

This project aims to assist market participants to make better decisions to determine liquidity pockets where the yield per dollar is higher than average volume. Thus allowing LPs to determine opportunities in Uniswap V3 to provide liquidity around calculated ticks and earn highest yield on their dollar. 

By design every swap event contains ex-ante active liquidity in the current tick range. We parsed these on-chain swap events, calculated fee accrued from each swap to the current providers and how much liquidity was active in during the swap. Hence creating a new metric named highest yield per dollar. In our dashboards users can check aggregated highest yield per dollar and also individual swaps, the ex-ante liquidity during that swap and many more assisting statistics. We heavily relied on python and its libraries such as pyspark, pandas and streamlit to create this structure. 

Further Improvements: Due to computational costs only select number of pools were analyzed, this can be expanded into any existing pool. Additionally, many rebalancing strategies can be built on this simple idea. For example, just like a momentum strategy one may choose to quote a wide range of ticks in case of trending yield per dollar & price. 
