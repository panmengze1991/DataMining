import pandas as pd
import time

inputPre = 'G:/DataMining/output/step1-subway/SPTCC-201504'
# inputPre = 'data/SPTCC-201504'
outputPre = 'G:/DataMining/output/step2-regular-subway/'
fileType = '.csv'
outputName = 'step2_result'
# indexWorkDay = ['06']


indexWorkDay = ['13', '14', '15', '20', '21', '22']


# 返回筛选后的合适用户数据集
def do_filter(group):
    # if len(set(group['type'].values)) == 1 and '地铁' in group['type'].values:
    #     return group
    if len(group) == 4:
        # try:
        #     new_group = group.copy()
        #     new_group.loc[:, 'station'] = group.loc[:, 'station'].apply(lambda x: x[x.index('号') + 2:len(x)])
        #     # group.station = group.station.apply(lambda x: x[x.index('号') + 2:len(x)])
        #     # group = group.station.apply(lambda x: x['station'][x.index('号线') + 2:len(x)])
        #     return new_group
        # except ValueError or AttributeError:
        #     print('error')
        # return None
        return group


# 获取步骤2的数据
def get_step2_data(index):
    chunks = []

    # 配置路径
    index_str = str(index)
    input_path = inputPre + index_str + fileType

    # 读取csv
    # data = pd.read_csv(input_path, header=None,
    #                    names=['id', 'date', 'time', 'station', 'type', 'money', 'discount'], low_memory=False)
    data = pd.read_csv(input_path, usecols=['id', 'station'], low_memory=False)

    # 重新组合data_group
    data_group = data.groupby('id')
    print(input_path + 'group by id ok')

    # count = 0
    for name, group in data_group:
        chunks.append(do_filter(group))
        # count += 1
        # if count == 1000:
        #     print("1000 apply")
        #     count = 0

    df = pd.concat(chunks, ignore_index=True)
    df2 = df.copy()
    df2.loc[:, 'station'] = df.loc[:, 'station'].apply(lambda x: x[x.index('号') + 2:len(x)])
    return df2
    # df.to_csv(output_path, index=None)


if __name__ == "__main__":

    result_buffer = []

    for fileIndex in indexWorkDay:
        start_time = time.time()
        # 获取到每个文件的每天有且只有两次乘车、四个站点的用户数据
        result_temp = get_step2_data(fileIndex)

        output_path = outputPre + outputName + fileType
        print('step1_result' + str(fileIndex) + 'complete. Saving Data...  use ' + str(time.time() - start_time) + "s")

        # 将数据写到输出文件中
        # step2_result.to_csv(outputPre + outputName + fileType)

        # 将数据存入内存中
        result_buffer.append(result_temp)

    # 结束循环，写入文件
    step2_result = pd.concat(result_buffer, ignore_index=True)
    step2_result.to_csv(outputPre + outputName + fileType, encoding='gbk', index=False, header=False)
