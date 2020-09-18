import math
from tqdm import tqdm

'''============清洗[abv]欄位==========='''
def abvWash(datas):
    error_list = []
    fixed_list = []
    new_data = []
    print(f'開始清洗abv欄位')
    for data in tqdm(datas):
        
        abv = data['abv']
        try: 
            #   轉換abv欄位型態為float
            if type(abv) != float:
                data['abv'] = float(abv)

            #   NaN to null
            if math.isnan(abv):
                fixed_list.append(data['name'])
                data['abv'] = 'null'

            #   proof to abv
            if abv >= 80:
                fixed_list.append(data['name'])
                data['abv'] = data['abv']/2

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