import pandas as pd
import h2o
from h2o.estimators.gbm import H2OGradientBoostingEstimator

con = h2o.connect(url='http://192.168.5.208:54321/')

csv_data = pd.read_csv('股票数据/处理后数据/processed_601857.csv', encoding='utf8')
csv_data['earn'] = csv_data['20_closing_price'] > csv_data['closing_price']*1.2
csv_data_ = h2o.H2OFrame(csv_data)
model = H2OGradientBoostingEstimator(model_id='stock_601857', nfolds=10,
distribution = "bernoulli", ntrees = 2000, max_depth = 10,
learn_rate = 0.4, histogram_type = "UniformAdaptive",
min_split_improvement = 0.000001,
balance_classes = False, seed = 52345,
stopping_rounds = 5, stopping_metric = 'AUC', stopping_tolerance = 0.001,
col_sample_rate = 0.6, col_sample_rate_per_tree = 0.6,
col_sample_rate_change_per_level = 0.6, sample_rate = 0.85, min_rows = 100,
)

traning_data, test_data = csv_data_.split_frame(ratios=[0.8], destination_frames=["train_frame", "test_data"])
csv_data.keys()
model.train(x=['closing_price', 'upping_ratio',
       'changing_ratio', 'volume', 'upping_ratio1',
       'upping_ratio2', 'upping_ratio3', 'upping_ratio4', 'upping_ratio5',
       'A_index_closing_price', 'A_index_upping_money', 'A_index_upping_ratio',
       'A_index_volume', 'A_index_volume_money', 'B_index_closing_price',
       'B_index_upping_money', 'B_index_upping_ratio', 'B_index_volume',
       'B_index_volume_money', 'top50_index_closing_price',
       'top50_index_upping_money', 'top50_index_upping_ratio',
       'top50_index_volume', 'top50_index_volume_money',
       'sh_index_closing_price', 'sh_index_upping_money',
       'sh_index_upping_ratio', 'sh_index_volume', 'sh_index_volume_money',
       'creating_index_closing_price', 'creating_index_upping_money',
       'creating_index_upping_ratio', 'creating_index_volume',
       'creating_index_volume_money', 'creating_all_closing_price',
       'creating_all_upping_money', 'creating_all_upping_ratio',
       'creating_all_volume', 'creating_all_volume_money',
       'con_B_index_closing_price', 'con_B_index_upping_money',
       'con_B_index_upping_ratio', 'con_B_index_volume',
       'con_B_index_volume_money', 'hushen_top300_closing_price',
       'hushen_top300_upping_money', 'hushen_top300_upping_ratio',
       'hushen_top300_volume', 'hushen_top300_volume_money',
       'shen_R_closing_price', 'shen_R_upping_money', 'shen_R_upping_ratio',
       'shen_R_volume', 'shen_R_volume_money', 'shen_con_closing_price',
       'shen_con_upping_money', 'shen_con_upping_ratio', 'shen_con_volume',
       'shen_con_volume_money', 'shen_all_closing_price',
       'shen_all_upping_money', 'shen_all_upping_ratio', 'shen_all_volume',
       'shen_all_volume_money'],
            y='earn', training_frame=traning_data)



test_result = model.predict(test_data=test_data)
