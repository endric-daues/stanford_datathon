import pandas as pd 
import numpy as np 

def process(df):
    df.loc[:,'date'] = df.date_range_start.apply(lambda x:pd.to_datetime(x).date())

    features = ['date','origin_census_block_group','median_home_dwell_time','median_percentage_time_home','median_non_home_dwell_time','mean_home_dwell_time','mean_non_home_dwell_time']
    features_s = ['completely_home_device_count','part_time_work_behavior_devices','full_time_work_behavior_devices','delivery_behavior_devices']

    for feature in features_s:
        df.loc[:,feature] = df.loc[:,feature] / df.device_count
        df.loc[:,feature] = df[feature].apply(lambda x: round(x,5))

    
    
    df_features = df[features + features_s]
    
    df_features.loc[:,'origin_census_block_group'] = df_features.origin_census_block_group.apply(lambda x: str(x).zfill(12))

    df_features.loc[:,'state'] = df_features.origin_census_block_group.apply(lambda x: x[:2])
    df_features.loc[:,'county'] = df_features.origin_census_block_group.apply(lambda x: x[2:4])

    return df_features

if __name__ == '__main__':
    
    count=0
    with open("social_mobility_Data/combined/features.csv","a+") as targetfile:

        header = 'origin_census_block_group,date_range_start,date_range_end,device_count,distance_traveled_from_home,bucketed_distance_traveled,median_dwell_at_bucketed_distance_traveled,completely_home_device_count,median_home_dwell_time,bucketed_home_dwell_time,at_home_by_each_hour,part_time_work_behavior_devices,full_time_work_behavior_devices,destination_cbgs,delivery_behavior_devices,median_non_home_dwell_time,candidate_device_count,bucketed_away_from_home_time,median_percentage_time_home,bucketed_percentage_time_home,mean_home_dwell_time,mean_non_home_dwell_time,mean_distance_traveled_from_home'.split(',')
        
        header_new = ['date', 'origin_census_block_group', 'median_home_dwell_time','median_percentage_time_home', 'median_non_home_dwell_time','mean_home_dwell_time', 'mean_non_home_dwell_time','completely_home_device_count', 'part_time_work_behavior_devices','full_time_work_behavior_devices', 'delivery_behavior_devices', 'state','county']
        targetfile.write(','.join(header_new)+'\n')
        
        chunksize = 500000
        
        for chunk in pd.read_csv('social_mobility_data/combined/combined.csv',chunksize=chunksize,header=None,names=header):
            print(count)
            count+=1
            
            df_processed = process(chunk)
            df_processed.loc[:,:]=df_processed.astype(str)
        
            for row in df_processed.head().values:
                targetfile.write(','.join(list(row))+'\n')

    
