#Импорт
import os
import dill
from datetime import datetime

#Абсолютные пути для Airflow
global_path = os.environ.get('PROJECT_PATH', '..')

#Подгрузка последней модели-прогнозера:
def actualModel():
    path = f'{global_path}/data/models/'
    files = os.listdir(path)
    files =[os.path.join(path,file) for file in files]
    with open(max(files, key=os.path.getctime),'rb') as f:
        model = dill.load(f)
    return model

#Парсинг тестовых данных в DataFrame:
def ReadDataTest():
    import pandas as pd
    df = pd.DataFrame()
    path = f'{global_path}/data/test'
    dataTest = os.listdir(path)
    dataTest = [os.path.join(path,test) for test in dataTest]
    for data in dataTest:
        subFrame = pd.read_json(data,typ='series')
        df = df._append(subFrame,ignore_index=True)
    return df

#Предикция и сохранение результата:
def predict():
    model = actualModel()
    data = ReadDataTest()
    data['predict_price_category'] = model.predict(data)
    data.to_csv(f'{global_path}/data/predictions/prediktor_{datetime.now().strftime("%Y%m%d%H%M")}.csv')


if __name__ == '__main__':
    predict()
