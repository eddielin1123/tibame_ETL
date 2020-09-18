import json
from redis import StrictRedis, ConnectionPool
import redis
import pymysql
import pymysql.cursors
import googletrans
import sys


class Find_or_Update():

	#   初始設定: Redis連接MySQL  
	def __init__(self, keyword):

		#	Redis 連線設定
		pool = redis.ConnectionPool(host='35.194.165.27', port=6379, db=0, decode_responses=True)
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
			tag_list = []
			if tmp['official_content'] == '空值':
				tmp['official_content'] = '暫無資料'
			if tmp['abv'] == 'null':
				tmp['abv'] = '暫無資料'
			if tmp['year'] == 'null':
				tmp['year'] = '暫無資料'
			if tmp['img'] == 'null':
				tmp['img'] = '暫無資料'
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

		#	設置key value過期時間
		self.r.expire(self.keyword,60)

		return json.loads(self.r.get(self.keyword))
	
	def main_name(self):

		#	判斷該Key是否存在於Redis
		if not self.r.exists(self.keyword):
			print('Redis尚無資料 將從MySQL查詢並存入Redis')
			return self.__redis_set(self.mysql_select_name())
		else:
			print('Redis搜尋成功!')
			return json.loads(self.r.get(self.keyword))



class editor():
	
	def __init__(self, line_data, dbdata):
		self.data = line_data
		self.dbdata = dbdata
		
	def edit_name(self):
		self.data['contents']['header']['contents'][0]['text'] = self.dbdata['name']
		r = self.data['contents']['header']['contents'][0]['text']
		return r

	def edit_tag(self):
		tmp_list = []
		for i, tag in enumerate(self.dbdata['tag']):
			self.data['contents']['header']['contents'][1]['contents'][i]['action']['label'] = tag
			self.data['contents']['header']['contents'][1]['contents'][i]['action']['text'] = f'風味：{tag}'
			tmp_list.append(self.data['contents']['header']['contents'][1]['contents'][i])
		self.data['contents']['header']['contents'][1]['contents'] = tmp_list
		r = self.data['contents']['header']['contents'][1]['contents']
		return r
	
	def edit_URL(self):
		self.data['contents']['hero']['url'] = self.dbdata['img']
		self.data['contents']['hero']['action']['uri'] = self.dbdata['img']
		r = self.data['contents']['hero']['action']['uri']
		return r
	
	def edit_abv(self):
		self.data['contents']['body']['contents'][0]['contents'][0]['contents'][1]['text'] = self.dbdata['abv']
		r = self.data['contents']['body']['contents'][0]['contents'][0]['contents'][1]['text']
		return r
	
	def edit_year(self):
		self.data['contents']['body']['contents'][0]['contents'][0]['contents'][3]['text'] = self.dbdata['year']
		r = self.data['contents']['body']['contents'][0]['contents'][0]['contents'][3]['text']
		return r
	
	def edit_type(self):
		self.data['contents']['body']['contents'][0]['contents'][1]['contents'][1]['text'] = self.dbdata['type']
		r = self.data['contents']['body']['contents'][0]['contents'][1]['contents'][1]['text']
		return r

	def edit_bc(self):
		self.data['contents']['body']['contents'][0]['contents'][2]['contents'][1]['text'] = self.dbdata['brand_country']
		r = self.data['contents']['body']['contents'][0]['contents'][2]['contents'][1]['text']
		return r
	
	def edit_oc(self):
		self.data['contents']['body']['contents'][0]['contents'][3]['contents'][1]['text'] = self.dbdata['official_content']
		r = self.data['contents']['body']['contents'][0]['contents'][3]['contents'][1]['text']
		return r
	
	def edit_reply(self):
		self.data['contents']['footer']['contents'][0]['action']['text'] = '評論'+self.dbdata['name']
		r = '評論'+self.dbdata['name']
		return r

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

def name_api(keyword):
	# try:
	obj = Find_or_Update(keyword)
	obj_data = obj.main_name()[0]
	with open('whiskey_name_example.txt', 'r', encoding='utf-8') as f:
		data = json.load(f)
		edi = editor(data, obj_data)
		edi_data = edi.edit_all()
		with open('whiskey_name_output.json', 'w', encoding='utf-8') as f2:
			json.dump(edi_data, f2, indent = 4, ensure_ascii=False)
	return edi_data
	# except IndexError as e:
	# 	print('搜尋失敗')
	# 	pass

if __name__ == '__main__':
	try:
		keyword = str(sys.argv[1])
		name_api(keyword)
	except IndexError:
		print('ERROR: 請輸入欲搜尋酒名\n使用方法: python Redis_linebot_API(name_whiskey).py "酒名"')
		
		
	