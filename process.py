import pandas as pd
from multiprocessing import Pool

inputPre = 'data/SPTCC-20150'
outputPre = 'newData/SPTCC-20150'
fileType = '.csv'


def get_clean_data(index):
    loop = True
    chunks = []

    index_str = str(index)
    input_path = inputPre + index_str + fileType
    output_path = outputPre + index_str + fileType

    reader = pd.read_csv(input_path, encoding='gbk', header=None,
                         names=['id', 'date', 'time', 'station', 'type', 'money', 'discount'],
                         chunksize=50000, iterator=True)
    print(input_path + ' load is ok')

    data = pd.read_csv(input_path, encoding='gbk', header=None,
                       names=['id', 'date', 'time', 'station', 'type', 'money', 'discount'])
    d1 = data.groupby('id')['type'].value_counts()
    print(input_path + 'group by id ok')

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
            print(input_path + '---' + str(i))
        except StopIteration:
            loop = False
            print(input_path + 'Iteration is stopped!')

    df = pd.concat(chunks, ignore_index=True)
    df.to_csv(output_path, index=None)


if __name__ == "__main__":
    get_clean_data(401)
    # pool = Pool(16)
    #
    # for index in range(401, 431, 1):
    #    index = str(index)
    #    pool.apply_async(get_clean_data, (index,))
    #
    # pool.close()
    # pool.join()
