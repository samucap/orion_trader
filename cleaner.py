# read, year/month
import os, glob
import numpy as np
import pandas as pd
import pyarrow as pa

def runner():
    for year in sorted(os.listdir(path='./flatfiles')):
    #os.mkdir("./us-stock-day-aggs-v1")
        files = sorted(glob.glob(f'./flatfiles/{year}/*/*'))
        print(f'Processing {len(files)} files, year: {year}')
        for csvFile in files:
            fileName = csvFile[20:]
            with pd.read_csv(csvFile, compression='gzip', chunksize=1000, parse_dates=True) as reader:
                tmp = []
                for chunk in reader:
                    chunk.insert(0, "date", pd.Series([fileName[:-7] for _ in range(chunk.shape[0])], dtype=pd.ArrowDtype(pa.timestamp('ns'))), allow_duplicates=True)
                    tmp.append(chunk.convert_dtypes(dtype_backend='pyarrow'))
                    
                df = pd.concat(tmp)
                print('.........\n', df.info())
                print(f'{df.head(15)}')
                #pd.to_csv(f'./us-stock-day-aggs-v1/{fileName[:4]}.csv.gz', mode='a', compression='gzip', header=withHead)
        break


if __name__ == '__main__':
    runner()
