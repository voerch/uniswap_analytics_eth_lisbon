from pyspark.sql import functions as f
from pyspark.sql.window import Window
import sys

raw_swaps_df =  spark.read.table("uniswap_v3_ethereum.pool_event_swap")
pool_creations_df =  spark.read.table("uniswap_v3_ethereum.factory_1_event_poolcreated")
token_infos_df =  spark.read.table("tokens_ethereum.erc20")
prices_df = spark.read.table("prices_uniswap.usd_minute")

# Lowercase the 'pool' column in pool_creations_df
pool_creations_df = pool_creations_df.withColumn('pool', f.lower(f.col('pool')))
pool_creations_df = pool_creations_df.withColumn('token0', f.lower(f.col('token0')))
pool_creations_df = pool_creations_df.withColumn('token1', f.lower(f.col('token1')))

# Join pool information like fees and ticks.
swaps_w_fees_df = raw_swaps_df.join(pool_creations_df.select('pool', 'fee', 'tickSpacing', 'token0', 'token1'), raw_swaps_df.contract_address == pool_creations_df.pool, 'left')


# Join token information like symbol and decimals.
swaps_w_fees_df = swaps_w_fees_df.join(token_infos_df, 
                                        swaps_w_fees_df.token0 == token_infos_df.contract_address, 
                                        'left') \
                                  .withColumnRenamed('symbol', 'symbol0') \
                                  .withColumnRenamed('decimals', 'decimals0')

# Drop the extra 'contract_address' column from the first join
swaps_w_fees_df = swaps_w_fees_df.drop(token_infos_df.contract_address)

# Join for token1
swaps_w_fees_df = swaps_w_fees_df.join(token_infos_df, 
                                        swaps_w_fees_df.token1 == token_infos_df.contract_address, 
                                        'left') \
                                  .withColumnRenamed('symbol', 'symbol1') \
                                  .withColumnRenamed('decimals', 'decimals1')

# Drop the extra 'contract_address' column from the second join
swaps_w_fees_df = swaps_w_fees_df.drop(token_infos_df.contract_address)



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


# Filters w.r.t. the token universe. 
swaps_w_fees_df = swaps_w_fees_df.dropna(subset=['decimals0', 'decimals1'])

# Convert fee from basis points to fraction
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee_fraction', f.col('fee') / f.lit(10000))

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

# Ensure the time columns are in a compatible format
swaps_w_fees_df = swaps_w_fees_df.withColumn("evt_block_time_min", f.date_trunc("minute", f.col("evt_block_time")))


# Join for token0
swaps_w_fees_df = swaps_w_fees_df.alias('a').join(
    prices_df.alias('b'), 
    (f.col('a.token0') == f.col('b.contract_address')) & 
    (f.col('a.evt_block_time_min') == f.col('b.minute')), 
    'left_outer'
).select('a.*', f.col('b.price').alias('token0_price'))

swaps_w_fees_df = swaps_w_fees_df.drop('b.contract_address', 'b.minute')

swaps_w_fees_df = swaps_w_fees_df.alias('a').join(
    prices_df.alias('b'), 
    (f.col('a.token1') == f.col('b.contract_address')) & 
    (f.col('a.evt_block_time_min') == f.col('b.minute')), 
    'left_outer'
).select('a.*', f.col('b.price').alias('token1_price'))

swaps_w_fees_df = swaps_w_fees_df.drop('b.contract_address', 'b.minute')

# Calculate USD value of token0 and token1 reserves
swaps_w_fees_df = swaps_w_fees_df.withColumn('token0_reserve_usd', f.col('amount0') * f.col('token0_price'))
swaps_w_fees_df = swaps_w_fees_df.withColumn('token1_reserve_usd', f.col('amount1') * f.col('token1_price'))

# Calculate USD value of the fee
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee0_usd', f.when(f.col('amount0') > 0, f.col('fee0') * f.col('token0_price')).otherwise(0))
swaps_w_fees_df = swaps_w_fees_df.withColumn('fee1_usd', f.when(f.col('amount1') > 0, f.col('fee1') * f.col('token1_price')).otherwise(0))

# Calculate yield per dollar
swaps_w_fees_df = swaps_w_fees_df.withColumn('yield_per_dollar', (f.col('fee0_usd') + f.col('fee1_usd')) / (f.col('token0_reserve_usd') + f.col('token1_reserve_usd')))