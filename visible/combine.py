import pandas as pd

inputWork = "../newData/station_result_work_4"
inputSunday = "../newData/station_result_sunday_4"
outputPath = "../newData/station_result_combine_"
fileType = ".csv"

# 新的label个数
combineType = 6

# 融合之前的label定义：
# work：  label：1 忽略，普通站点
#         label：2 工作(非常多)
#         label：3 工作(中等)
#         label：0 住宿
# sunday：label：1 中级商圈
#         label：2 顶级商圈
#         label：0,3 次级和底级，忽略
# 融合之后新的label定义：
# 0. 无存在感点 work(1)+sunday(0,3)
# 1. 纯工作 work(2,3)+sunday(0,3)
# 2. 纯住宿 work(0)+sunday(0,3)
# 3. 工作商圈 work(2,3)+sunday(1,2)
# 4. 住宿商圈 work(0)+sunday(1,2)
# 5. 纯商圈 work(1)+sunday(1,2)

# 第一层下标work，第二层下标Sunday，对应Label
label_map = [[2, 4, 4, 2],
             [0, 5, 5, 0],
             [1, 3, 3, 1],
             [1, 3, 3, 1]]

if __name__ == "__main__":
    # 工作日聚类结果
    df_workday = pd.read_csv(inputWork + fileType, low_memory=False)

    # 周日聚类结果
    df_sunday = pd.read_csv(inputSunday + fileType, low_memory=False)

    df = df_workday
    df['sunday_label'] = df_sunday.label

    # df_new = [df.loc[df['label'] == 1].loc[df['sunday_label'] == (0 or 3)],
    #           df.loc[df['label'] == (2 or 3)].loc[df['sunday_label'] == (0 or 3)],
    #           df.loc[df['label'] == 0].loc[df['sunday_label'] == (0 or 3)],
    #           df.loc[df['label'] == (2 or 3)].loc[df['sunday_label'] == (1 or 2)],
    #           df.loc[df['label'] == 0].loc[df['sunday_label'] == (1 or 2)],
    #           df.loc[df['label'] == 1].loc[df['sunday_label'] == (1 or 2)]]

    df_new = []
    for index, station in df.iterrows():
        station.label = label_map[station.label][station.sunday_label]
        df_new.append(station)
    df_new = pd.DataFrame(df_new)
    print(df_new)
    del df_new['sunday_label']
    df_new.to_csv(outputPath + str(combineType) + fileType, encoding='utf-8', index=False)
