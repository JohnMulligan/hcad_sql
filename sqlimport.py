import mysql.connector
import requests, zipfile, io
import re
import os

import time

cnx = mysql.connector.connect(user='python', password='python123!',host='127.0.0.1')
cursor=cnx.cursor()

homedir='sql_tmp'

if not os.path.exists(homedir):
	os.makedirs(homedir)

d=open("urls.txt")
t=d.read()
d.close()

urls=[i.strip() for i in t.split("\n") if i!='']

#print(urls)

years=range(2005,2023)

logfile=open("timings.txt",'a')

timing_step_size=10000

minimum_disk=10000000

for year in years:
	print(year)
	db_name="HCAD_%d" %year
	createstatement="create database %s;" %db_name
	
	try:
		cursor.execute(createstatement)
		cnx.commit()
	except:
		pass
	
	for url in urls:
		u=re.sub("/YYYY/","/"+str(year)+"/",url)
		datasetname=re.search("[^/]+?(?=\.zip)",url).group(0)
		print(datasetname)
		print("downloading",u)
		r = requests.get(u)
		z = zipfile.ZipFile(io.BytesIO(r.content))
		z.extractall(homedir)
		tsvs=[i for i in os.listdir(homedir) if i.endswith('.txt')]
		for tsv in tsvs:
			tablename=re.sub("\.txt","",tsv)
			print(tablename)
			filepath=os.path.join(homedir,tsv)

			with open(filepath) as f:
				firstline=f.readline().strip()
				
			headers=[i for i in firstline.split('\t')]
			
			columnstr=",\n".join([i + " TEXT" for i in headers])
			
			createstatement="create table %s.%s_%s (%s);" %(db_name,datasetname,tablename,columnstr)
			
			#print(createstatement)
			
			cursor.execute(createstatement)
			
			cnx.commit()
			
			qmarks=','.join(["%s" for i in headers])
			columnstr2=",".join(headers)
			
			
			
			with open(filepath,'r',encoding='latin-1') as f:
				st=time.time()
				c=0
				for line in f:
					vals=[i for i in line.strip().split('\t')]
					
					insertstatement="insert into %s.%s_%s (%s) values (%s)" %(db_name,datasetname,tablename,columnstr2,qmarks)
					
					#print(insertstatement,vals)
					
					try:
						cursor.execute(insertstatement,vals)
						cnx.commit()
					except:
						print("failed on",vals)
					c+=1
					
					if c%timing_step_size==0:
						s = os.statvfs('/')
						df=(s.f_bavail * s.f_frsize) / 1024
						writestring='\t'.join([str(i) for i in [year,datasetname,tablename,c,time.time()-st,df]])
						print(writestring)
						logfile.write(writestring)
						logfile.write('\n')
						
						if df < minimum_disk:
							print("below minimum disk space. stopping.")
							exit()
				
				os.remove(filepath)
				
logfile.close()
cnx.close()
