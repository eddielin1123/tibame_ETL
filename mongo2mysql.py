
import math
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData, exc

from Mongo import All_clean

if __name__ == "__main__":

    datas = All_clean.clean()

    engine = create_engine('mysql+pymysql://root:zdtb0626@35.194.165.27/Whiskey')
    Base = declarative_base()
    md = MetaData(bind=engine)

    #   create session of MySQL
    Session = sessionmaker(bind=engine)
    session = Session()
    
    #   refer to the table
    whiskey = Table('whiskey', md, autoload=True, autoload_with=engine)

    #   update data to MySQL DB
    for i, data in enumerate(datas):
        
        try:
            session.execute(whiskey.insert(), {'id':i, 'name':data['name'], 'abv':str(data['abv']), 'brand_country':data['brand_country'], 'official_content':data['official_content'], 'type':data['type'], 'year':data['year']})
        
        except exc.IntegrityError:

            stmt = whiskey.update().where(whiskey.c.id == i).\
                values({
                    'name':data['name'],
                    'abv':data['abv'],
                    'brand_country':data['brand_country'],
                    'official_content':data['official_content'],
                    'type':data['type'],
                    'year':data['year']
                })

            session.execute(stmt, data)

    
    print(f'已 更新/上傳 {len(datas)}筆資料')

    session.commit()
    session.close

