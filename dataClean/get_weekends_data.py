import pandas as pd
import numpy as np
import time

inputPre = 'G:/DataMining/output/step1-subway/SPTCC-201504'
outputPre = '../newData/weekends_201504'
fileType = '.csv'
idPath = '../data/step3_result'
indexWeekends = ['18', '19', '25', '26']


def is_data_available(user_id, id_df):
    if len(id_df.id[id_df.id == user_id]) == 0:
        return False
    else:
        return True


def get_available_data(index, id_df):
    # 构造输入输出路径
    input_path = inputPre + index + fileType
    output_path = outputPre + index + fileType
    record_id_group = []
    # 读取文件
    record_df = pd.read_csv(input_path,
                            dtype={'id': np.int64},
                            chunksize=100000,
                            low_memory=False)

    for chunk in record_df:
        record_id_group.append(chunk)

    record_id_group = pd.concat(record_id_group, ignore_index=True)
    record_id_group = record_id_group.groupby(by='id')

    print('get data start')
    available_df = []
    for user_id, user_record_df in record_id_group:
        if is_data_available(user_id, id_df):
            available_df.append(user_record_df)
    print('get data end')

    data = pd.concat(available_df, ignore_index=True)
    data.to_csv(output_path, index=None, encoding='gbk')


if __name__ == "__main__":
    # 有效的id集合
    id_list = pd.read_csv(idPath + fileType, encoding='gbk', names=['id'], header=None, low_memory=False,
                          dtype={'id': np.int64})

    total_start = time.time()

    for file_index in indexWeekends:
        start_time = time.time()
        print('获取工作日出行有效数据：file = ', file_index, 'start at ' + time.asctime(time.localtime(time.time())))
        get_available_data(file_index, id_list)
        print('获取工作日出行有效数据：end at ' + time.asctime(time.localtime(time.time())))
        print('use ' + str(time.time() - start_time) + "s\n ")

    print('all end & use ' + str(time.time() - total_start) + "s\n")
