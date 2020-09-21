
import math
import pymysql
from sqlalchemy import create_engine, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData, exc

from Mongo.method import name_clean


if __name__ == "__main__":

    datas = name_clean.nameWash()
    print('test')
    mysql_ip = '0.0.0.0'
    engine = create_engine(f'mysql+pymysql://root:zdtb0626@{mysql_ip}/Whiskey')
    Base = declarative_base()
    md = MetaData(bind=engine)
    print('test1')
    #   create session of MySQL
    Session = sessionmaker(bind=engine)
    session = Session()
    
    #   refer to the table
    whiskey = Table('whiskey', md, autoload=True, autoload_with=engine)

    #   update data to MySQL DB
    for i, data in enumerate(datas):
        
        # try:
        #     session.execute(whiskey.insert(), {'id':i, 'name':data['name'], 'abv':str(data['abv']), 'brand_country':data['brand_country'], 'official_content':data['official_content'], 'type':data['type'], 'year':data['year']})
        
        # except exc.IntegrityError:

        #     stmt = whiskey.update().where(whiskey.c.id == i).\
        #         values({
        #             'name':data['name'],
        #             'abv':data['abv'],
        #             'brand_country':data['brand_country'],
        #             'official_content':data['official_content'],
        #             'type':data['type'],
        #             'year':data['year']
        #         })

        #     session.execute(stmt, data)

        try:
            update_stmt = whiskey.update()\
                        .where(whiskey.c.name == data['name'])\
                        .values(item_name = data['whiskey_name'])
            session.execute(update_stmt)
            
            # for row in session.query(whiskey).all():
                # print(row)
            # print(f'更新 {session.query(whiskey).all()} ROW')
        except Exception as e:
            print('無此資料')

    
    # print(f'已 更新/上傳 {len(datas)}筆資料')

    session.commit()
    session.close