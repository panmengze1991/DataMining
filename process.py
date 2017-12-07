import pandas as pd
from multiprocessing import Pool


def get_clean_data(index):
    
    index = str(index)
    loop = True
    chunks = []
    reader = pd.read_csv('data/SPTCC-20150'+str(index)+'.csv',encoding='gbk',header=None,
                  names=['id', 'date', 'time', 'station', 'type', 'money', 'discount'],
                         chunksize=50000, iterator=True)
    print(index+' load is ok')

    data = pd.read_csv('data/SPTCC-20150'+str(index)+'.csv',encoding='gbk',header=None,
                  names=['id', 'date', 'time', 'station', 'type', 'money', 'discount'])
    d1 = data.groupby('id')['type'].value_counts()
    print(index+'groupby id ok')

    del data

    i = 0
    while loop:
        try:
            df = pd.DataFrame(next(reader))
            d2 = pd.DataFrame(df.id.apply(lambda x:
                                             ('地铁' in d1[x]) and
                                             (not (('公交' in d1[x]) or
                                                   ('出租' in d1[x]) or
                                                   ('轮渡' in d1[x]) or
                                                   ('P+R停车场' in d1[x])))))
            df = df[d2.id]
            chunks.append(df)
            i += 1
            print(index+'---'+str(i))
        except StopIteration:
            loop = False
            print(index+'Iteration is stopped!')

    df = pd.concat(chunks, ignore_index=True)
    df.to_csv('newData/SPTCC-20150'+str(index)+'.csv', index=None)



if __name__ == "__main__":

   get_clean_data(401)
   # pool = Pool(16)

    #for index in range(401, 431, 1):
    #    index = str(index)
    #    pool.apply_async(get_clean_data, (index,))

   # pool.close()
   # pool.join()
