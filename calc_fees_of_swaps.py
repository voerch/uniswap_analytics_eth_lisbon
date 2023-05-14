from pyspark.sql import functions as f
from pyspark.sql.window import Window
import sys

raw_swaps_df =  spark.read.table("uniswap_v3_ethereum.pool_event_swap")
pool_creations_df =  spark.read.table("uniswap_v3_ethereum.factory_1_event_poolcreated")
token_infos_df =  spark.read.table("tokens_ethereum.erc20")
prices_df = spark.read.table("prices_uniswap.usd_day")

# Example pools of USDC/ETH, USDC/USDT and MATIC/ETH.
pool_addresses = [
    '0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801'
]

# 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640
#    '0x290a6a7460b308ee3f19023d2d00de604bcf5b42',

raw_swaps_df = raw_swaps_df.where(f.col('contract_address').isin(pool_addresses))


pool_creations_df = pool_creations_df.drop(f.col('blockchain'))
token_infos_df = token_infos_df.drop(f.col('blockchain'))
prices_df = prices_df.drop(f.col('blockchain'))

# Lowercase the 'pool' column in pool_creations_df
pool_creations_df = pool_creations_df.withColumn('pool', f.lower(f.col('pool')))
pool_creations_df = pool_creations_df.withColumn('token0', f.lower(f.col('token0')))
pool_creations_df = pool_creations_df.withColumn('token1', f.lower(f.col('token1')))


# Join pool information like fees and ticks.
swaps_w_fees_df = raw_swaps_df.join(pool_creations_df.select('pool', 'fee', 'tickSpacing', 'token0', 'token1'), raw_swaps_df.contract_address == pool_creations_df.pool, 'left')


swaps_w_fees_df = swaps_w_fees_df.alias('a').join(token_infos_df.alias('b'), 
                                                  f.col('a.token0') == f.col('b.contract_address'), 'left') \
                                        .withColumnRenamed('symbol', 'symbol0') \
                                        .withColumnRenamed('decimals', 'decimals0')
                                        
swaps_w_fees_df = swaps_w_fees_df.drop(f.col('b.contract_address'))

# Use alias and join token information like symbol and decimals for token1.
swaps_w_fees_df = swaps_w_fees_df.alias('a').join(token_infos_df.alias('b'), 
                                                  f.col('a.token1') == f.col('b.contract_address'), 'left') \
                                        .withColumnRenamed('symbol', 'symbol1') \
                                        .withColumnRenamed('decimals', 'decimals1')

swaps_w_fees_df = swaps_w_fees_df.drop(f.col('b.contract_address'))

# 1. Calculate price from tick
swaps_w_fees_df = swaps_w_fees_df.withColumn('price', f.pow(f.lit(1.0001), f.col('tick')))

# 2. Calculate low tick and top tick with adjustment for negative ticks
swaps_w_fees_df = swaps_w_fees_df.withColumn('low_tick', f.when(f.col('tick') < 0, ((f.col('tick') / f.col('tickSpacing')).cast('integer') - 1) * f.col('tickSpacing')).otherwise((f.col('tick') / f.col('tickSpacing')).cast('integer') * f.col('tickSpacing')))
swaps_w_fees_df = swaps_w_fees_df.withColumn('top_tick', f.when(f.col('tick') < 0, (f.col('tick') / f.col('tickSpacing')).cast('integer') * f.col('tickSpacing')).otherwise(((f.col('tick') / f.col('tickSpacing')).cast('integer') + 1) * f.col('tickSpacing')))

# 3. Calculate price of low tick and top tick
swaps_w_fees_df = swaps_w_fees_df.withColumn('low_tick_price', f.pow(f.lit(1.0001), f.col('low_tick')))
swaps_w_fees_df = swaps_w_fees_df.withColumn('top_tick_price', f.pow(f.lit(1.0001), f.col('top_tick')))

# 4. Calculate square root of price
swaps_w_fees_df = swaps_w_fees_df.withColumn('sqrt_price', f.sqrt('price'))
swaps_w_fees_df = swaps_w_fees_df.withColumn('sqrt_low_tick_price', f.sqrt('low_tick_price'))
swaps_w_fees_df = swaps_w_fees_df.withColumn('sqrt_top_tick_price', f.sqrt('top_tick_price'))

# Calculate token reserves for low and top ticks
swaps_w_fees_df = swaps_w_fees_df.withColumn('token0_reserve', 
                                             (f.col('liquidity') * (f.col('sqrt_top_tick_price') - f.col('sqrt_price')) / 
                                             (f.col('sqrt_top_tick_price') * f.col('sqrt_price'))) / f.pow(f.lit(10), f.col('decimals0')))

swaps_w_fees_df = swaps_w_fees_df.withColumn('token1_reserve', 
                                             (f.col('liquidity') * (f.col('sqrt_price') - f.col('sqrt_low_tick_price'))) / f.pow(f.lit(10), f.col('decimals1')))

# Filters w.r.t. the token universe. 
swaps_w_fees_df = swaps_w_fees_df.dropna(subset=['decimals0', 'decimals1'])

# Convert fee from basis points to fraction
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee_fraction', f.col('fee') / f.lit(1000000))

# Cast amounts to double
swaps_w_fees_df = swaps_w_fees_df.withColumn('amount0', swaps_w_fees_df['amount0'].cast('double'))
swaps_w_fees_df = swaps_w_fees_df.withColumn('amount1', swaps_w_fees_df['amount1'].cast('double'))

# Calculate fees
swaps_w_fees_df = swaps_w_fees_df.withColumn(
    'fee0',
    f.when(f.col('amount0') > 0, f.col('amount0') * f.col('fee_fraction') / f.pow(f.lit(10.0), f.col('decimals0'))).otherwise(0)
)

swaps_w_fees_df = swaps_w_fees_df.withColumn(
    'fee1',
    f.when(f.col('amount1') > 0, f.col('amount1') * f.col('fee_fraction') / f.pow(f.lit(10.0), f.col('decimals1'))).otherwise(0)
)

stablecoins_list = [
    '0x4fabb145d64652a948d72533023f6e7a623c7c53', #'BUSD'
    '0x6b175474e89094c44da98b954eedeac495271d0f', #'DAI'
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', #'USDC'
    '0xdac17f958d2ee523a2206206994597c13d831ec7', #'USDT'
]

# Ensure the time columns are in a compatible format
swaps_w_fees_df = swaps_w_fees_df.withColumn("evt_block_time_day", f.date_trunc("day", f.col("evt_block_time")))


# Join for token0
swaps_w_fees_df = swaps_w_fees_df.alias('a').join(
    prices_df.alias('b'), 
    (f.col('a.token0') == f.col('b.contract_address')) & 
    (f.col('a.evt_block_time_day') == f.col('b.day')), 
    'left_outer'
).select('a.*', f.col('b.price').alias('token0_price'))

swaps_w_fees_df = swaps_w_fees_df.drop('b.contract_address', 'b.day')

swaps_w_fees_df = swaps_w_fees_df.alias('a').join(
    prices_df.alias('b'), 
    (f.col('a.token1') == f.col('b.contract_address')) & 
    (f.col('a.evt_block_time_day') == f.col('b.day')), 
    'left_outer'
).select('a.*', f.col('b.price').alias('token1_price'))

swaps_w_fees_df = swaps_w_fees_df.drop('b.blockchain', 'b.contract_address', 'b.day')

# Calculate USD value of token0 and token1 reserves
swaps_w_fees_df = swaps_w_fees_df.withColumn('token0_reserve_usd', f.col('amount0') * f.col('token0_price'))
swaps_w_fees_df = swaps_w_fees_df.withColumn('token1_reserve_usd', f.col('amount1') * f.col('token1_price'))

# Calculate USD value of the fee
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee0_usd', f.when(f.col('amount0') > 0, f.col('fee0') * f.col('token0_price')).otherwise(0))
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee1_usd', f.when(f.col('amount1') > 0, f.col('fee1') * f.col('token1_price')).otherwise(0))

# Calculate yield per dollar
swaps_w_fees_df = swaps_w_fees_df.withColumn('yield_per_dollar', (f.col('fee0_usd') + f.col('fee1_usd')) / (f.col('token0_reserve_usd') + f.col('token1_reserve_usd')))


# Calculate USD value of token0 and token1 reserves
swaps_w_fees_df = swaps_w_fees_df.withColumn('token0_reserve_usd', f.col('token0_reserve') * f.col('token0_price'))
swaps_w_fees_df = swaps_w_fees_df.withColumn('token1_reserve_usd', f.col('token1_reserve') * f.col('token1_price'))

# Calculate USD value of the fee
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee0_usd', f.when(f.col('amount0') > 0, f.col('fee0') * f.col('token0_price')).otherwise(0))
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee1_usd', f.when(f.col('amount1') > 0, f.col('fee1') * f.col('token1_price')).otherwise(0))

# Calculate yield per dollar
swaps_w_fees_df = swaps_w_fees_df.withColumn('yield_per_dollar', (f.col('fee0_usd') + f.col('fee1_usd')) / (f.col('token0_reserve_usd') + f.col('token1_reserve_usd')))

