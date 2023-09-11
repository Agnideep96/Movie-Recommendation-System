from flask import Flask,render_template,request
import os
from pathlib import Path
from google.cloud import storage
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName('PySpark DataFrame From RDD').getOrCreate()
from decimal import *
import pandas as pd
import tensorflow as tf

app=Flask(__name__)
# dir_path = Path('gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample')
# file_name = 'sample1.txt'
# file_path = dir_path.joinpath(file_name)
bucket_name='dataproc-staging-europe-west1-543422417702-vaxdbckx'
object_name='tfidf_sample/sample.txt'

@app.route('/', methods=['POST','GET'])

def calculate():
    bmi=''
    if request.method=='POST' and 'weight' in request.form and 'height' in request.form:
        movieInput=request.form.get('weight')
        movieName=movieInput+" "
        movieRating=float(request.form.get('height'))
        #fetch movie Id from movie name
        data = pd.read_csv('gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample/CleanedTable.csv', sep = ',', header = 0, nrows=9000000)
        result=data[data['title']==movieName]
        result0=result[' movieId']
        result1=result0.drop_duplicates()
        movieId=int(result1.index[0])


        mId=str(movieId)
        mR=str(movieRating)
        # Merging the strings
        movie=mId+','+mR
        # Writing into files
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_string(movie)

               #input: (m_i, m_j), (sim, avgi,avgj) llista de new_movies
        def check(data, keys):
            if str(data[0][0]) in keys:
                return [(data[0][0],data[1][1]), (data[0][1], data[1][2])]
            return [(data[0][1], data[1][2]), (data[0][0],data[1][1])]

        #input: m_j, llista de (m_i, avgi, sim, avgj), r_j
        def build(data):
            res = []
            for m_i, avgi, sim, avgj in data[1][0]:
                res.append(((m_i, avgi),(data[0], avgj, sim, data[1][1])))
            return res

        #input: (m_i, avgi), llista de (m_j, avgj, sim, r_j)
        def pred(data):
            res = 0
            up = 0
            down = 0
            for m_j, avgj, sim, r_j in data[1]:
                up += sim*(r_j - avgj)
                down += sim
            res = (up/down) + data[0][1]
            return res

        #df = (m1,m2),(sim,avg1,avg2)
        #df2 = (m, r)
        df = sc.textFile('gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample/out1')
        df1 = df.map(lambda x: x.strip('(')).map(lambda x: x.strip(')')).map(lambda x: x.split(','))
        df2 = sc.textFile('gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample/sample.txt')
        df3 = df2.map(lambda x: x.split(',')).map(lambda x: (int(x[0]), float(x[1])))
        df4 = df1.map(lambda x: ((int(x[0]), int(x[1])), (float(x[2]), float(x[3]), float(x[4]))))
        keys = df2.keys().collect()

        filter_movies = df4.filter(lambda x: str(x[0][0]) in keys or str(x[0][1]) in keys).map(lambda x: (check(x, keys)[0][0], (check(x, keys)[1][0], check(x, keys)[1][1], x[1][0], check(x, keys)[0][1])))
        #filter_movies = m_j, (m_i, avg_i, sim, avg_j)
        add_all = filter_movies.groupByKey().mapValues(list).leftOuterJoin(df3).flatMap(lambda x: [((key[0], key[1])) for key in build(x)]).groupByKey().mapValues(list)
        #add_all = (m_i, avgi), llista de (m_j, avgj, sim, r_j)
        res = add_all.map(lambda x: (x[0][0], pred(x))).sortBy(lambda x: -x[1]).map(lambda x: x[0]).take(10)
        sc.parallelize(res).saveAsTextFile('gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample/out2.txt')

        folder_path = './out2.txt'
        file_list = tf.io.gfile.glob("gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample/out2.txt/part*")
        main_dataframe = pd.read_csv(file_list[0], header = None)

        for i in range(1,len(file_list)):
            data = pd.read_csv(file_list[i], header = None)
            main_dataframe = pd.concat([main_dataframe, data], axis = 0)
        main_dataframe.to_csv('gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample/final.txt', header = None, index = None, sep = ',')

        # find the movie name from the movie id
        choice = res
        df = pd.read_csv('gs://dataproc-staging-europe-west1-543422417702-vaxdbckx/tfidf_sample/CleanedTable.csv', sep = ',', header = 0)

        df.drop_duplicates(subset=['title'], inplace=True)

        print(df)
        new= df[' movieId'].isin(choice)
        df1=df[new]
        print(df1)
        result=df1['title'].values.tolist()
        print(result)
        bmi=result
     #RETURNING ARRAY OF MOVIES TO HTML           
    return render_template('index.html', bmi=bmi)
if __name__=='__main__':
    app.run(host='0.0.0.0', port=5001)
