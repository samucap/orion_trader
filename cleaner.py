# read, year/month
import os, glob, io
import numpy as np
import pandas as pd
import pyarrow as pa
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import talib.abstract as ta
load_dotenv()

def runner(minio=None):
    lastclose = []
    lasthi = []
    lastlo = []   

    for year in range(2020, 2026):
        files = sorted(glob.glob(f'./flatfiles/{year}/*/*'))
        n = len(files)
        print(f'Processing {n} files for year {year}')
        bstream = io.BytesIO()
        tmp = []
        for i in range(n):
            f = files[i]
            fName = f[20:]
            tmp.append(pd.read_csv(f, compression='gzip', chunksize=1000))

        df = pd.concat([item.read() for item in tmp])
        s = df['window_start'].map(lambda x: pd.to_datetime(x, utc=True).strftime("%Y-%m-%d"))
        df.insert(0, "date", s, allow_duplicates=True)
        # TODO: TA indicators
        add_technical_indicators(df, lastclose, lasthi, lastlo)

        bstream.write(df.to_parquet(engine="pyarrow"))
        nbytes = bstream.getbuffer().nbytes
        print(f'uploading year {year} total: {nbytes/1000000}')

        bstream.seek(0)
        result = minio.put_object("us-stock-day-aggs-v1", f'{year}.parquet', bstream, nbytes, content_type="application/octet-stream")
        print("Success ============== ", result.__dict__)
        
        bstream.seek(0)
        bstream.truncate()

    resp = minio.get_object('us-stock-day-aggs-v1', '2021.parquet')
    data = io.BytesIO(resp.read())
    newDF = pd.read_parquet(data)
    print('checking >>>>>>>>>>>>> ', newDF.info())
    print(newDF.head())
    print(newDF.tail())
    print('dun')

def add_technical_indicators(df: pd.DataFrame, lastclose, lasthi, lastlo):
    close = df['close']
    hi = df['high']
    lo = df['low']
    if lastclose:
        close = pd.concat([lastclose, close])
    if lasthi:
        hi = pd.concat([lasthi, hi])
    if lastlo:
        hi = pd.concat([lastlo, lo])

    # MACD (returns macd, signal, hist)
    df['macd'], _, _ = ta.MACD(close)
    
    # Bollinger Bands (upper, middle, lower)
    df['boll_ub'], _, df['boll_lb'] = ta.BBANDS(close)
    
    # RSI 30
    df['rsi_30'] = ta.RSI(close, timeperiod=30)
    
    # CCI 30
    df['cci_30'] = ta.CCI(hi, lo, close, timeperiod=30)
    
    # DX 30
    df['dx_30'] = ta.DX(hi, lo, close, timeperiod=30)
    
    # Close 30 SMA
    df['close_30_sma'] = ta.SMA(close, timeperiod=30)
    
    # Close 60 SMA
    df['close_60_sma'] = ta.SMA(close, timeperiod=60)

    # TODO: handle less than 60
    lastclose = close[:-60]
    lasthi = hi[:-60]
    lastlo = lo[:-60]    

def createBucket(client, bucketName):
    try:
        if not client.bucket_exists(bucketName):
            client.make_bucket(bucketName)
            print(f'Bucket {bucketName} created successfully.')
        else:
            print(f'Bucket {bucketName} already exists.')
    except S3Error as e:
        print(f'Error: {e}')

def makeMinIO():
    c = Minio(
        os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ROOT_USER', 'key'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'pw'),
        secure=False
    )

    createBucket(c, 'us-stock-day-aggs-v1')
    return c


if __name__ == '__main__':
    minio = makeMinIO()
    runner(minio)

# reading
#resp = minio.get_object('us-stock-day-aggs-v1', "2020.parquet")
#data = io.BytesIO(resp.read())
#newDF = pd.read_parquet(data, engine="pyarrow")
#print('>>>>>>>>>>>>> describe')
#newDF.describe()
#print(f'>>>>>>>>>>>>> info ', newDF.info())
#print(newDF.head(10))
#print(newDF.tail(10))
