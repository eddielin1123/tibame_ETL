from tqdm import tqdm
from Mongo import db_data

'''============清洗[name]欄位==========='''
def nameWash(datas=db_data):
    error_list = []
    fixed_list = []
    new_data = []
    print(f'開始清洗name欄位')
    for data in tqdm(datas):
        try:
            name = data['name']
        except Exception as e:
            error_list.append(data)
            data_name = data['whiskey_name']
            # print(f'預期外錯誤: {e}\n發現錯誤資料: {data_name}')

        if 'Ol??Major' in name.strip():
            data['name'] = "OL\' MAJOR"
            fixed_list.append(data['name'])

        elif 'Kaiy?' in name.strip():
            data['name'] = 'Kaiyo'
            fixed_list.append(data['name'])

        elif  'Ballantine?s' in name.strip():
            data['name'] = 'Ballantine\'s'
            fixed_list.append(data['name'])

        elif 'KROB?R' in name.strip():
            data['name'] = 'Krobar'
            fixed_list.append(data['name'])

        elif 'Muirhead?s' in name.strip():
            data['name'] = 'MUIRHEAD\'S'
            fixed_list.append(data['name'])

        elif 'Gibson?s' in name.strip():
            data['name'] = 'Gibson\'s'
            fixed_list.append(data['name'])

        elif 'Hy?go Prefecture' in name.strip():
            data['name'] = 'Hyogo Prefecture'
            fixed_list.append(data['name'])

        elif 'Gibson?s' in name.strip():
            data['name'] = 'Gibson\'s'
            fixed_list.append(data['name'])

        elif 'Ko?' in name.strip():
            data['name'] = 'KO\'OLAU'
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