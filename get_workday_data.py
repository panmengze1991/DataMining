import pandas as pd
import numpy as np
import time

inputPre = 'G:/DataMining/output/step1-subway/SPTCC-201504'
# inputPre = 'data/SPTCC-201504'
outputPre = 'newData/work_day_201504'
fileType = '.csv'
idPath = 'data/step3_result'
indexWorkDay = ['13', '14', '15', '16', '17', '20', '21', '22', '23', '24']


def is_data_available(user_id, id_df):
    # for id_one in id_df.id:
    #     # print('id ='+id_one+'user_id ='+user_id+'==? ', id_one == user_id)
    #     # print(len(id_one))
    #     # print(len(user_id))
    #     if user_id == id_one:
    #         # print('find!')
    #         return True
    # return False
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

    # print('group start')
    record_id_group = pd.concat(record_id_group, ignore_index=True)

    record_id_group = record_id_group.groupby(by='id')
    # print('group end')

    print('get data start')
    available_df = []
    # is_data_available('402653184', id_df)
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
    # id_list.id = id_list[0].astype(np.int64)
    # id_format = id_list.id.apply(lambda x: x.strip())
    total_start = time.time()
    # get_available_data('06', id_list)

    for file_index in indexWorkDay:
        start_time = time.time()
        print('获取工作日出行有效数据：file = ', file_index, 'start at ' + time.asctime(time.localtime(time.time())))
        get_available_data(file_index, id_list)
        print('获取工作日出行有效数据：end at ' + time.asctime(time.localtime(time.time())))
        print('use ' + str(time.time() - start_time) + "s\n ")

    print('all end & use ' + str(time.time() - start_time) + "s\n")
