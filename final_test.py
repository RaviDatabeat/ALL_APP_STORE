
# import pandas as pd
# data = pd.read_parquet("final_all_stores_file_testing__.parquet")
# print(data.shape)

# data.to_csv('checkingthedat.csv')


# val = []

# for i in data:
#     val.append([i.get('app_name'), i.get('bundle_id'), i.get('url'), i.get('developer_url')])



# df = pd.DataFrame(val, columns=['app_name','bundle_id','url','developer_url'])
# print(df.head())
# ==========================================================

# testing

import pandas as pd

# Sample DataFrame 1
df1 = pd.DataFrame({
    "app_name": ["App One", "App Two"],
    "bundle_id": ["com.app.one", "com.app.two"],
    "domain": ["example.com", "example.org"]
})

# Sample DataFrame 2
df2 = pd.DataFrame({
    "app_name": ["App Three", "App Four"],
    "bundle_id": ["com.app.three", "com.app.four"],
    "domain": ["sample.com", "sample.org"]
})

# print(df1.shape)
# print(df2.shape)

# ads_df = pd.concat([None,None], ignore_index=True) 
# print(ads_df.shape)
# print(ads_df)

# =====================================
# lst = ['dataaa',None]
# if any(item is not None for item in lst):
#     print('data')
# else:
#     print('no data ')


# ================

# lst = [{'app_name': 'Glow Eve Period Tracker', 'bundle_id': '1002275138', 'url': 'https://itunes.apple.com/lookup?id=1002275138', 'developer_url': 'http://glowing.com'}, {'app_name': 'travel-thru-history', 'bundle_id': '101036', 'url': 'https://channelstore.roku.com/details/29fef91ea9123ed10aaab390e3cff3fa:654c4fa0fc0fa1bd243474b1b3d49731/travel-thru-history', 'developer_url': 'https://www.travelingsocotra.com/'}]

# for i in lst:
#     if i['app_name']=='Glow Eve Period Tracker':
#         print(i['bundle_id'])
#     # print(i['app_name']=='Glow Eve Period Tracker')



# ================

# df = pd.DataFrame({
#     "app_name": ["App1"],
#     "bundle_id": ["com.app1"]
# })

# df2 = pd.DataFrame({
#     "app_name": ["App2", "App3", "App2", "App3"],
#     "bundle_id": ["com.app2", "com.app3", "com.app2", "com.app3"]
# })

# df_expanded = pd.concat([df] * len(df2), ignore_index=True)

# dff  = pd.concat([df_expanded,df2], axis=1)
# print(dff)

# =================================
