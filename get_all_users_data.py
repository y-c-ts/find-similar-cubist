import pandas as pd
import MySQLdb

MYSQL_USERNAME ='####'
MYSQL_PASSWORD = '####'
MYSQL_HOST ='#####'
MYSQL_DATABASE ='######'

class GetDataFromDB(object):

    def __init__(self):
        self.conn = MySQLdb.connect(
            user = MYSQL_USERNAME,
            passwd = MYSQL_PASSWORD,
            host = MYSQL_HOST,
            db = MYSQL_DATABASE)
        self.WCA_ID_arr = self.get_all_WCA_ID_arr()
        self.result_df = pd.DataFrame()
        #self.run()

    def run(self):
        result_df = pd.DataFrame()
        count = 0
        for wca_id in self.WCA_ID_arr:
            df = self.make_user_df(wca_id)
            converted_df = self.convert_df_shape(df, wca_id)
            result_df = result_df.append(converted_df, sort=True)
            count+=1
            if count % 100 == 0:
                print(count)
            if count % 5000 == 0:
                save_name =  str(count) + '_result.csv'
                result_df.to_csv(save_name)
                result_df = pd.DataFrame()
        result_df = self.fill_nan(result_df)
        self.result_df = result_df.reset_index(drop=True)

    def get_all_WCA_ID_arr(self):
        sql = "SELECT id FROM Persons"
        df = pd.read_sql_query(sql, self.conn)
        return df['id'].values

    def make_user_df(self, WCA_ID):
        sql = "SELECT personId,eventId,worldRank,countryId,gender,name \
        FROM RanksAverage \
        INNER JOIN Persons ON Persons.ID = RanksAverage.personId \
        WHERE personId ='{wca_id}'".format(wca_id = WCA_ID)
        df = pd.read_sql_query(sql, self.conn)
        return df

    def convert_df_shape(self, df, WCA_ID):
        tranformed_df = df.T
        dic = dict(zip(tranformed_df.loc['eventId'], tranformed_df.loc['worldRank']))
        df_row = pd.DataFrame(dic,index=[WCA_ID])
        try:
            df_row['id'] = WCA_ID
            df_row['gender'] = df['gender'].iloc[0]
            df_row['country'] = df['countryId'].iloc[0]
            return df_row
        except:
            pass

    def fill_nan(self, df):
        fill_columns = df.columns[df.dtypes == float]
        for column in fill_columns:
            df[column] = df[column].fillna(df[column].max() + 10000)
        return df
