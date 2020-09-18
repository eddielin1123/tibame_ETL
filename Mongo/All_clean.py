from method import *
from mongo_init import MyMongo
from tqdm import tqdm

db = MyMongo('Whiskey', 'Whiskey')
data = db.findAll()
coll = db.DB_coll

def clean():
    new_data = name_clean.nameWash(data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = abv_clean.abvWash(data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = type_clean.typeWash(data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = content_clean.contentWash(data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = brand_clean.brandWash(data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = year_clean.yearWash(data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)

    print('資料清理完畢')
    return new_data
