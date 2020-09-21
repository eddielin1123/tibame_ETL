from Mongo.method import name_clean, abv_clean, type_clean, content_clean, brand_clean, year_clean
from Mongo import db, db_data, coll
from tqdm import tqdm


def clean():
    global db_data
    new_data = name_clean.nameWash(db_data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = abv_clean.abvWash(db_data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = type_clean.typeWash(db_data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = content_clean.contentWash(db_data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = brand_clean.brandWash(db_data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)
    
    new_data = year_clean.yearWash(db_data)
    for data in tqdm(new_data):
        name = data['whiskey_name']
        coll.replace_one({'whiskey_name':f'{name}'}, data)

    print(f'{len(new_data)}資料清理完畢')
    return new_data
