from pymongo import MongoClient

class MyMongo():
    '''=============初始連線設定=============='''
    def __init__(self, db, coll):
        
        self.db = db
        self.coll = coll 
        self.user = 'eddie'# 輸入email帳號 (只需要'@'前面的)
        self.uri  = f'mongodb://{self.user}:eb102@35.194.165.27:27017/'
        self.client = MongoClient(self.uri) 

        self.database = self.client[db]
        self.DB_coll = self.database[coll]
        
    '''============呼叫所有資料============='''
    def findAll(self):
        return self.DB_coll.find({},{'_id': False})