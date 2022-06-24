#import streamlit as st
import requests
import pandas as pd
import json

def highlight_duplicates(vals):
    uniq = set()
    duplicates = set()
    for v in vals:
        if v in uniq:
            duplicates.add(v)
        else:
            uniq.add(v)
    return ['background-color: red' if v in duplicates else '' for v in vals]

positions = pd.read_csv('reports/positions.csv')
pending = pd.read_csv('reports/pending.csv')
pending['value'] = [(trigger if trigger > 0 else price)*quantity for price,trigger,quantity in zip(pending.price, pending.trigger_price, pending.quantity)]

pos_grouped = positions.groupby(['symbol']).agg({
    'quantity': sum,
    'average_buy_price': 'mean',
    'average_sell_price': 'mean',
    'net_amount': sum,
    'ltp': 'mean'
    }).reset_index()
pos_grouped['bep'] = pos_grouped.eval('net_amount/net_quantity').round(2).abs()
pending_grouped = pending.groupby(['trading_symbol', 'order_side',
    'order_type']).agg({
    'quantity': sum,
    'value': sum,
    })
pending_grouped['avg_price'] = pending_grouped.eval('value/quantity')

pending_grouped = pending_grouped.unstack(level='order_type').reset_index().rename(
            columns={'trading_symbol': 'symbol'})


grp = pos_grouped.merge(pending_grouped, on=['symbol'])
columns = ['symbol', 'net_quantity', 'bep', 'ltp', ('avg_price', 'LIMIT'),
        ('avg_price', 'SL'), 'product']

grp[columns].to_csv('reports/reports.csv')

'''
st.write(
        grp[columns].style.\
        highlight_null(null_color='red').\
        apply(highlight_duplicates, subset=['symbol'])
        )

st.text('Values highlighted in red are errors')
'''
