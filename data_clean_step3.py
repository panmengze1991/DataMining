import pandas as pd
import time

inputPre = 'G:/DataMining/output/step2-regular-subway/'
outputPre = 'G:/DataMining/output/step3-formatted-data/'
fileType = '.csv'
inputName = 'step2_result'
outputName = 'step3_result'


def get_step3_format():
    data_frames = []
    input_path = inputPre + inputName + fileType
    print('start read')
    # 读取csv
    chunks = pd.read_csv(input_path,
                         header=None,
                         names=['a', 'id', 'date', 'time', 'station', 'type', 'money', 'discount'],
                         chunksize=100000,
                         low_memory=False)
    print('end read')

    for chunk in chunks:
        data_frames.append(chunk)

    del chunks
    # df = pd.concat(data_frames, ignore_index=True)

    # print('group by id start')
    # dg = df.groupby('id')
    # data_frames = []
    # print('sort start')
    # for name, group in dg:
    #     # 排序
    #     # sorted_group = group.sort_values(['date', 'time'], ascending=True)
    #     try:
    #         group.loc[:, 'station'] = group.station.apply(lambda x: x[x.index('号线') + 2:len(x)])
    #     except ValueError:
    #         print('value error at :' + name)
    #
    #     data_frames.append(group)
    #     print('group filter')
    #
    # print('concat start')

    df = pd.concat(data_frames, ignore_index=True)
    df_id = pd.Series(list(set(df['id'].values)))
    # df.drop(['a'], 1, inplace=True)
    return df_id

    # return df.groupby('id')


if __name__ == "__main__":
    start_time = time.time()

    df_step3_pre = get_step3_format()
    df_step3_pre.to_csv(outputPre + outputName + fileType, encoding='utf_8_sig', index=False, header=False)
