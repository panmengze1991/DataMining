import pandas as pd
from multiprocessing import Pool

inputPre = 'data/SPTCC-20150'
outputPre = 'newData/SPTCC-20150'
fileType = '.csv'


# 返回筛选后的合适用户数据集
def do_filter(group):
    if len(set(group['type'].values)) == 1 and '地铁' in group['type'].values:
        return group


def get_clean_data(index):
    chunks = []

    # 配置路径
    index_str = str(index)
    input_path = inputPre + index_str + fileType
    output_path = outputPre + index_str + fileType

    # 读取csv
    data = pd.read_csv(input_path, encoding='gbk', header=None,
                       names=['id', 'date', 'time', 'station', 'type', 'money', 'discount'], low_memory=False)

    # 将读取的data按照id重新组合为data_group
    data_group = data.groupby('id')
    print(input_path + 'group by id ok')

    for name, group in data_group:
        chunks.append(do_filter(group))

    df = pd.concat(chunks, ignore_index=True)
    df.to_csv(output_path, index=None)


if __name__ == "__main__":
    # pool = Pool(12)

    for fileIndex in range(401, 431, 1):
        get_clean_data(fileIndex)
        # pool.apply_async(get_clean_data, (fileIndex,))

    # pool.close()
    # pool.join()
