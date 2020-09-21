from tqdm import tqdm
import math

'''============清洗[year]欄位==========='''
def yearWash(datas):
    error_list = []
    fixed_list = []
    new_data = []
    print(f'開始清洗year欄位')
    # year = data['year']
    for data in tqdm(datas):
        try:
            if 'Year' in str(data['year']):
                data['year'] = str(data['year']).replace('Year','').strip()
                fixed_list.append(data['name'])

            elif type(data['year']) != str and math.isnan(data['year']):
                # if math.isnan(data['year']):
                data['year'] = 'null'
                fixed_list.append(data['name'])

        except Exception as e:
            error_list.append(data)
            data_name = data['whiskey_name']
            print(f'預期外錯誤: {e}\n發現錯誤資料: {data_name}')

        new_data.append(data)
    print(f'已修改{len(list(set(fixed_list)))}筆資料')
    
    if error_checker(error_list) == True:
        return new_data
    else:
        return f'尚有{len(error_list)}錯誤資料未處理'

'''============錯誤紀錄============='''
def error_checker(error_list):
    if error_list == []:
        return True
    else:
        return f'尚有{len(error_list)}筆資料未處理'