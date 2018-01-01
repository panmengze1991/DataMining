import pandas as pd
import numpy as np
import time

inputStation = '../newData/station_result_combine_'
# inputStation = '../newData/station_result_work_'
inputLocation = '../data/交通出行.csv'
outputPath = '../newData/station_map_combine_'
# outputPath = '../newData/station_map_work_'
fileType = '.csv'

numOfType = 6


def get_location(name, location_df, position):
    for ix, location in location_df.iterrows():
        if name in location['name']:
            return location[position]
    return np.nan


if __name__ == "__main__":
    # 站点集合
    station_list = pd.read_csv(inputStation + str(numOfType) + fileType, low_memory=False)
    station_list.station = station_list.station.apply(lambda x: x.strip())

    # 读取DAT格式的站点信息
    location_list = pd.read_csv(inputLocation, header=None, names=['name', 'x', 'y'], encoding='gbk', low_memory=False)

    start_time = time.time()

    key = station_list.station.values
    value = station_list.label.values
    label_map = dict(zip(key, value))

    result = pd.DataFrame()
    result['name'] = station_list.station
    result['x'] = station_list.station
    result['y'] = station_list.station
    result['label'] = station_list.station

    result.x = result.x.apply(lambda name: get_location(name, location_list, 1))
    result.y = result.y.apply(lambda name: get_location(name, location_list, 2))
    result.label = result.label.apply(
        lambda name: label_map[name] if get_location(name, location_list, 1) is not np.nan else np.nan)

    print(result)
    for label in range(0, numOfType):
        result[result.label == label].to_csv(outputPath + str(numOfType) + "_type_" + str(label) + fileType,
                                             index=False)

    print('all end & use ' + str(time.time() - start_time) + "s\n")
