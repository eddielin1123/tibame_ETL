from tqdm import tqdm


'''============清洗[official_content]欄位==========='''
def contentWash(datas):
    error_list = []
    fixed_list = []
    new_data = []
    print(f'開始清洗official_content欄位')
    for data in tqdm(datas):
        try:
            oc = data['official_content']
        except Exception as e:
            error_list.append(data)
            data_name = data['whiskey_name']
            print(f'預期外錯誤: {e}\n發現錯誤資料: {data_name}')

        #   NaN check
        if type(oc) != str:
            data['official_content'] = 'null'
            fixed_list.append(data['name'])

        #   '?' remove   
        else:
            if '?' in oc:
                data['official_content'] = data['official_content'].replace('?','')
                fixed_list.append(data['name'])
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