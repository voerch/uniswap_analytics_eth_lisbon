import pandas as pd
from glob import glob

raw_files = glob("raw_data*")

for file in raw_files:
    swaps_w_fees_df = pd.read_csv(file)
    swaps_w_fees_df["fee_usd"] = swaps_w_fees_df.fee0_usd + swaps_w_fees_df.fee1_usd
    swaps_w_fees_df["reserves_usd"] = swaps_w_fees_df.token0_reserve_usd + swaps_w_fees_df.token1_reserve_usd

    swaps_w_fees_df["evt_block_time"] = pd.to_datetime(swaps_w_fees_df["evt_block_time"])
    swaps_w_fees_df.set_index("evt_block_time", inplace=True)
    swaps_w_fees_df.sort_index(inplace=True)

    resampled_df = swaps_w_fees_df.resample('30Min').agg({'symbol0' : 'last', 
                                                        'symbol1' : 'last', 
                                                        'pool' : 'last', 
                                                        'fee_usd' : 'sum', 
                                                        'reserves_usd' : 'mean',
                                                        'low_tick' : 'min',
                                                        'top_tick' : 'max'})


    resampled_df["yield_per_dollar"] = resampled_df.fee_usd / resampled_df.reserves_usd
    resampled_df = resampled_df.dropna()
    resampled_df.yield_per_dollar.plot()

    resampled_df.to_csv(f'processed_data_{resampled_df.symbol0[0]}_{resampled_df.symbol1[0]}.csv')

