import pandas as pd

swaps_w_fees_df = pd.read_csv('data_sample.csv')
swaps_w_fees_df["fee_usd"] = swaps_w_fees_df.fee0_usd + swaps_w_fees_df.fee1_usd
swaps_w_fees_df["reserves_usd"] = swaps_w_fees_df.token0_reserve_usd + swaps_w_fees_df.token1_reserve_usd

swaps_w_fees_df["evt_block_time"] = pd.to_datetime(swaps_w_fees_df["evt_block_time_day"])
swaps_w_fees_df.set_index("evt_block_time", inplace=True)

pools = swaps_w_fees_df.contract_address.unique()

for pool in pools:
    resampled_df = swaps_w_fees_df[swaps_w_fees_df.contract_address == pool]
    resampled_df = resampled_df.resample('30Min').agg({'symbol0' : 'last', 
                                                       'symbol1' : 'last', 
                                                       'pool' : 'last', 
                                                       'fee_usd' : 'sum', 
                                                       'reserves_usd' : 'mean',
                                                       'low_tick' : 'min',
                                                       'top_tick' : 'max'})
    resampled_df["yield_per_dollar"] = resampled_df.fee_usd / resampled_df.reserves_usd

resampled_df.dropna()
resampled_df.to_csv('sample_processed_data.csv')
