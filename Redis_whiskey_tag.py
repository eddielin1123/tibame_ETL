import json
from redis import StrictRedis, ConnectionPool
import redis
import pymysql
import pymysql.cursors
import googletrans
import sys

import logging


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

# Formatter
formatter = logging.Formatter('[%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s',datefmt='%Y-%m-%d %H:%M')

# FileHandler
file_handler = logging.FileHandler('mongoTomysql_record.log','a','utf_8_sig')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(level=logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class Find_or_Update():

	#   初始設定: Redis連接MySQL  
	def __init__(self, keyword):

		#	Redis 連線設定
		pool = redis.ConnectionPool(host='35.194.165.27', port=6379, db=1, decode_responses=True)
		self.r = redis.StrictRedis(connection_pool=pool)

		#	MySQL 連線與游標設定
		self.db = pymysql.connect(host='35.194.165.27', 
                                    db='Whiskey' ,
                                    charset='utf8mb4',
                                    user='root',
                                    password='zdtb0626')
		self.cursor = self.db.cursor()
		self.keyword = keyword
	
	#	Google翻譯
	def translate_tw(self, word):
		import googletrans
		translator = googletrans.Translator()
		return translator.translate(word, dest='zh-tw').text

	#	標籤 英轉中
	def tag_transform(self, tag):
		tag_dict = {'salted':'鹹味','earthy':'泥土味','floral':'花香','peaty':'泥煤香', 'spicy':'辛香', 'fruity':'果香', 'careal':'穀物', 'wood':'木質香', 'feinty':'辛辣香氣','sulphury':'礦物香'}
		return tag_dict[tag]
	
	#	搜尋標籤
	def mysql_select_tag(self):
		sql = f'SELECT name, IFNULL(abv, "暫無資料"), brand_country, IFNULL(official_content, "暫無資料"), type, IFNULL(year, "暫無資料"), img, IFNULL(tag_1, "null"), IFNULL(tag_2, "null"), IFNULL(tag_3, "null"), IFNULL(tag_4, "null")\
				FROM whiskey \
				WHERE tag_1="{self.keyword}" or tag_2="{self.keyword}" or tag_3="{self.keyword}" or tag_4="{self.keyword}"\
				ORDER BY RAND() \
				LIMIT 5'
		self.cursor.execute(sql)
		data_list = list(self.cursor.fetchall())
		return data_list
	
	#	搜尋酒名
	def mysql_select_name(self):
		sql = f"SELECT name, IFNULL(abv, '暫無資料'), brand_country, IFNULL(official_content, '暫無資料'), type, IFNULL(year, '暫無資料'), img, IFNULL(tag_1, 'null'), IFNULL(tag_2, 'null'), IFNULL(tag_3, 'null'), IFNULL(tag_4, 'null')\
				FROM whiskey \
				WHERE name LIKE '%{self.keyword}%' \
				ORDER BY RAND() \
				LIMIT 1"
		self.cursor.execute(sql)
		data_list = list(self.cursor.fetchall())
		return data_list

	#	Redis set from MySQL
	def __redis_set(self , data_list):

		new_data_list = []

		#	redis更新数据: hmset(key,{mapping})
		for data in data_list:
			tmp = {}
			tmp['name'] = data[0]
			tmp['abv'] = data[1]
			tmp['brand_country'] = data[2]
			tmp['official_content'] = self.translate_tw(data[3])
			tmp['type'] = data[4]
			tmp['year'] = data[5]
			tmp['img'] = data[6]
			if tmp['official_content'] == '空值':
				tmp['official_content'] = '暫無資料'
			if tmp['abv'] == 'null':
				tmp['abv'] = '暫無資料'
			if tmp['year'] == 'null':
				tmp['year'] = '暫無資料'
			if tmp['img'] == 'null':
				tmp['img'] = '暫無資料'

			tag_list = []
			if data[7] != 'null':
				tag_list.append(self.tag_transform(data[7]))
			if data[8] != 'null':
				tag_list.append(self.tag_transform(data[8]))
			if data[9] != 'null':
				tag_list.append(self.tag_transform(data[9]))
			if data[10] != 'null':
				tag_list.append(self.tag_transform(data[10]))
			tmp['tag'] = tag_list
			new_data_list.append(tmp)

		#	使用 Json dumps 將資料存入 Redis
		self.r.set(self.keyword, json.dumps(new_data_list))

		#	設置key value過期時間: 60秒
		self.r.expire(self.keyword,60)

		#	以json格式存入 需以同樣格式取出
		return json.loads(self.r.get(self.keyword))
	
	def main_name(self):

		#	判斷該Key是否存在於Redis
		if not self.r.exists(self.keyword):
			print('Redis尚無資料 將從MySQL查詢並存入Redis')
			return self.__redis_set(self.mysql_select_name())
		else:
			print('Redis搜尋成功!')
			return json.loads(self.r.get(self.keyword))

	def main_tag(self):

		#	判斷該Key是否存在於Redis
		if not self.r.exists(self.keyword):
			print('Redis尚無資料 將從MySQL查詢並存入Redis')
			return self.__redis_set(self.mysql_select_tag())
		else:
			print('Redis搜尋成功!')
			return json.loads(self.r.get(self.keyword))

#	編輯並生成Linebot前端呈現用json
class editor():
	
	def __init__(self, line_data, dbdata):
		self.data = line_data
		self.dbdata = dbdata
		
	def edit_name(self):
		self.data['header']['contents'][0]['text'] = self.dbdata['name']
		r = self.data['header']['contents'][0]['text']
		return r

	def edit_tag(self):
		tmp_list = []
		for i, tag in enumerate(self.dbdata['tag']):
			self.data['header']['contents'][1]['contents'][i]['action']['label'] = tag
			self.data['header']['contents'][1]['contents'][i]['action']['text'] = f'風味：{tag}'
			tmp_list.append(self.data['header']['contents'][1]['contents'][i])
		self.data['header']['contents'][1]['contents'] = tmp_list
		r = self.data['header']['contents'][1]['contents']
		return r
	
	def edit_URL(self):
		self.data['hero']['url'] = self.dbdata['img']
		self.data['hero']['action']['uri'] = self.dbdata['img']
		r = self.data['hero']['action']['uri']
		return r
	
	def edit_abv(self):
		self.data['body']['contents'][0]['contents'][0]['contents'][1]['text'] = self.dbdata['abv']
		r = self.data['body']['contents'][0]['contents'][0]['contents'][1]['text']
		return r
	
	def edit_year(self):
		self.data['body']['contents'][0]['contents'][0]['contents'][3]['text'] = self.dbdata['year']
		r = self.data['body']['contents'][0]['contents'][0]['contents'][3]['text']
		return r
	
	def edit_type(self):
		self.data['body']['contents'][0]['contents'][1]['contents'][1]['text'] = self.dbdata['type']
		r = self.data['body']['contents'][0]['contents'][1]['contents'][1]['text']
		return r

	def edit_bc(self):
		self.data['body']['contents'][0]['contents'][2]['contents'][1]['text'] = self.dbdata['brand_country']
		r = self.data['body']['contents'][0]['contents'][2]['contents'][1]['text']
		return r
	
	def edit_oc(self):
		self.data['body']['contents'][0]['contents'][3]['contents'][1]['text'] = self.dbdata['official_content']
		r = self.data['body']['contents'][0]['contents'][3]['contents'][1]['text']
		return r
	
	def edit_reply(self):
		self.data['contents']['footer']['contents'][0]['action']['text'] = '評論'+self.dbdata['name']
		r = '評論'+self.dbdata['name']
		return r
	
	#	main function for editor
	def edit_all(self):
		self.edit_name()
		self.edit_abv()
		self.edit_bc()
		self.edit_oc()
		self.edit_tag()
		self.edit_URL()
		self.edit_year()
		self.edit_type()
		self.edit_reply()
		return self.data

#	啟用兩個物件及方法 最後生成文件
def tag_api(keyword):
	# try:
	obj = Find_or_Update(keyword)
	obj_data_list = obj.main_tag()

	#	編碼無法使用utf-8 and I have no idea why
	with open('whiskey_tag_example.txt', 'r', encoding='windows-1252') as tag_file:
		file_data = json.load(tag_file)
		new_data_list = []
		for data, obj_data in zip(file_data['contents']['contents'], obj_data_list):
			edi = editor(data, obj_data)
			data=edi.edit_all()
			new_data_list.append(data)
		file_data['contents']['contents'] = new_data_list
		with open('whiskey_tag_output.json', 'w', encoding='utf-8') as f2:
			json.dump(file_data, f2, indent = 4, ensure_ascii=False)
	return file_data
	# except IndexError as e:
	# 	print('搜尋失敗')
	# 	pass

if __name__ == '__main__':
	try:
		keyword = str(sys.argv[1])
		tag_api(keyword)
	except IndexError:
		print('ERROR: 請輸入欲搜尋標籤\n使用方法: python Redis_linebot_API(name_whiskey).py "標籤"')

