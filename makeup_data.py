#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pandas as pd
import os
from tqdm import tqdm

csv_path = '股票数据'
stock_path = '股票'
exponent_path = '指数'
process_path = '处理后数据'

col_name_dict = {
    '日期': 'date',
    '股票代码': 'stock_code',
    '名称': 'stock_name',
    '收盘价': 'closing_price',
    '最高价': 'highest_price',
    '最低价': 'lowest_price',
    '开盘价': 'opening_price',
    '前收盘': 'yst_cloing_price',
    '涨跌额': 'upping_money',
    '涨跌幅': 'upping_ratio',
    '换手率': 'changing_ratio',
    '成交量': 'volume',
    '成交金额': 'volume_money',
    '总市值': 'total_price',
    '流通市值': 'com_price'
}

index_file_name_dict = {
    'A股指数': 'A_index',
    'B股指数': 'B_index',
    '上证50指数': 'top50_index',
    '上证指数': 'sh_index',
    '创业板指': 'creating_index',
    '创业板综': 'creating_all',
    '成份Ｂ指': 'con_B_index',
    '沪深300指数': 'hushen_top300',
    '深成指R': 'shen_R',
    '深证成指': 'shen_con',
    '深证综指': 'shen_all'
}

for file_name in tqdm(os.listdir('{}/{}'.format(csv_path, stock_path))):
    # file_name = os.listdir('{}/{}'.format(csv_path, stock_path))[0]
    csv_data = pd.read_csv('{}/{}/{}'.format(csv_path, stock_path, file_name), encoding='gbk')
    csv_data = csv_data.dropna(axis=0).reset_index(drop=True)
    csv_data.keys()
    csv_data = csv_data.drop(['最高价', '最低价', '开盘价', '前收盘', '涨跌额', '成交金额', '总市值', '流通市值'], axis=1)
    csv_data = csv_data.rename(columns=col_name_dict)

    days_later = 20
    csv_data['{}'.format('{}_closing_price'.format(days_later))] = csv_data[col_name_dict['收盘价']][days_later-1:, ].reset_index(drop=True)

    for i in range(1, 6):
        csv_data['{}{}'.format('upping_ratio', i)] = csv_data[col_name_dict['涨跌幅']][i:, ].reset_index(drop=True)
    csv_data = csv_data.dropna(axis=0).reset_index(drop=True)

    csv_data = csv_data.set_index(keys=col_name_dict['日期'])

    index_csv_names = os.listdir('{}/{}'.format(csv_path, exponent_path))

    for index_csv_name in index_csv_names:
        index_data = pd.read_csv('{}/{}/{}'.format(csv_path, exponent_path, index_csv_name), index_col='日期', encoding='gbk',
                                 usecols=['日期', '收盘价', '涨跌额', '涨跌幅', '成交量', '成交金额'])
        index_data.dropna()
        index_data.rename(columns={k: '{}_{}'.format(index_file_name_dict[index_csv_name.split('_')[0]], col_name_dict[k]) for k in index_data.keys()}, inplace=True)
        csv_data = csv_data.join(index_data, rsuffix=None, lsuffix=None)


    csv_data.to_csv('{}/{}/{}'.format(csv_path, process_path, 'processed_{}'.format(file_name)), encoding='utf8')


# os.system("shutdown -s -t 1")