import streamlit as st
import pandas as pd
import plotly.express as px

st.title('Swap and Aggregated Swap Data Display')

# create a navigation menu
page = st.sidebar.radio("Choose a page", ['Latest Data', 'Aggregated MATIC_WETH', 'Aggregated APE_WETH', 'Aggregated UNI_WETH', 'Swap MATIC_WETH', 'Swap APE_WETH', 'Swap UNI_WETH'])

if page == 'Latest Data':
    # read the last row from each dataframe
    matic_weth_df = pd.read_csv('processed_data_MATIC_WETH.csv').tail(1)
    ape_weth_df = pd.read_csv('processed_data_APE_WETH.csv').tail(1)
    uni_weth_df = pd.read_csv('processed_data_UNI_WETH.csv').tail(1)

    # append the dataframes together
    combined_df = pd.concat([matic_weth_df, ape_weth_df, uni_weth_df])

    # display the combined dataframe
    st.dataframe(combined_df)

else:
    if page == 'Aggregated MATIC_WETH':
        processed_df = pd.read_csv('processed_data_MATIC_WETH.csv')
    elif page == 'Aggregated APE_WETH':
        processed_df = pd.read_csv('processed_data_APE_WETH.csv')
    elif page == 'Aggregated UNI_WETH':
        processed_df = pd.read_csv('processed_data_UNI_WETH.csv')
    elif page == 'Swap MATIC_WETH':
        processed_df = pd.read_csv('raw_data_MATIC_WETH.csv')
        # calculate fee_usd
        processed_df['fee_usd'] = processed_df['fee0_usd'] + processed_df['fee1_usd']
    elif page == 'Swap APE_WETH':
        processed_df = pd.read_csv('raw_data_APE_WETH.csv')
        # calculate fee_usd
        processed_df['fee_usd'] = processed_df['fee0_usd'] + processed_df['fee1_usd']
    elif page == 'Swap UNI_WETH':
        processed_df = pd.read_csv('raw_data_UNI_WETH.csv')
        # calculate fee_usd
        processed_df['fee_usd'] = processed_df['fee0_usd'] + processed_df['fee1_usd']

    # convert the 'evt_block_time' column to datetime
    processed_df['evt_block_time'] = pd.to_datetime(processed_df['evt_block_time'])

    # set the 'evt_block_time' column as the index
    processed_df.set_index('evt_block_time', inplace=True)

    # display the whole dataframe
    st.dataframe(processed_df)

    # plot the required columns
    if 'Swap' in page:
        fig = px.scatter(processed_df, x=processed_df.index, y="fee_usd")
        st.plotly_chart(fig)
    else:
        fig = px.line(processed_df, y="fee_usd")
        st.plotly_chart(fig)
        fig = px.line(processed_df, y="reserves_usd")
        st.plotly_chart(fig)
        fig = px.line(processed_df, y="yield_per_dollar")
        st.plotly_chart(fig)
