import pandas as pd
import time

inputPre = 'G:/DataMining/output/step2-regular-subway/'
outputPre = 'G:/DataMining/output/step3-formatted-data/'
fileType = '.csv'
inputName = 'step2_result'
outputName = 'step3_result'


def is_available_id(user_id, group):
    # 只保留有两个站点的数据
    if len(set(group.loc[:, 'station'])) == 2 and len(group) > 20:
        return user_id


def get_step3_format():
    # 接收缓存读取数据
    data_frames = []
    # 接收验证成功的id
    available_id = []

    input_path = inputPre + inputName + fileType
    print('start read')
    # 读取csv
    chunks = pd.read_csv(input_path,
                         header=None,
                         names=['id', 'station'],
                         encoding='gbk',
                         chunksize=1000000,
                         low_memory=False)
    print('end read')

    for chunk in chunks:
        data_frames.append(chunk)

    del chunks

    # 合并数据
    print('start concat')
    df = pd.concat(data_frames, ignore_index=True)
    print('end concat')
    print('start group')
    # 按照id聚合
    id_group = df.groupby('id')
    print('end group')
    print('start filter')
    for user_id, group in id_group:
        # 遍历数据以达到获取有效id的效果
        a_id = is_available_id(user_id, group)
        if a_id is not None:
            available_id.append(a_id)
    print('end filter')

    df_id = pd.Series(available_id)
    return df_id


if __name__ == "__main__":
    start_time = time.time()
    # 获取
    available_id_series = get_step3_format()
    available_id_series.to_csv(outputPre + outputName + fileType, encoding='gbk', index=False)
    print('available_id_series complete. Saving Data...  use ' + str(time.time() - start_time) + "s")
