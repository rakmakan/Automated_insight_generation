import pandas as pd
import os


def file_formation(Locations):
    files = []
    for _ in os.listdir(Locations):
        if _.startswith('~$', 0) == False:
            files.append(_)

    cols = {}
    unique_cols = []
    for file in files:
        data = pd.read_excel(os.path.join(Locations, file))
        columns = data.columns.values.tolist()
        clean_cols = []
        for i in columns:
            i =  i.strip().lower().replace(' ', '_').replace('sum_', '').replace('left_','').replace('right_','')
            clean_cols.append(i)
            unique_cols.append(i)
        cols[file] = clean_cols

    unique_cols = list(set(unique_cols+['data_type', 'data_category']))
    op_data = pd.DataFrame(columns=unique_cols)


    op_data['data_type'] = [i.split('.')[0].split('(')[0] for i in files] 
    op_data['data_category'] = [i.split('.')[0].split('(')[1].replace(')','') for i in files] 

    for key , values in cols.items():
        for j in values:
            type_index = op_data.index[(op_data['data_type'] == key.split('.')[0].split('(')[0] )].tolist()[0]
            op_data.loc[type_index ,j] = 1

    op_data = op_data.fillna(0)

    op_data.to_csv("C:/Users/makanra2/Desktop/Kitchen/Insight/Classifier_file.csv", index=False)