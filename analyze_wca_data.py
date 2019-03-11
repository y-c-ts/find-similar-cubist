import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
EVENT_DICT = {}

class ClusterWCAData(object):
    def __init__(self, csv_file, pca_dimension, cluster_num, wca_id, country):
        self.wca_id = wca_id
        self.country = country
        self.rank_df = self.preprocess_rank_df(pd.read_csv(csv_file).drop_duplicates(subset='id'))
        self.pca_dimension = pca_dimension
        self.cluster_num = cluster_num
        self.similar_wca_id_list = []
        self.run()

    def run(self):
        target = self.rank_df['id'].values
        features_arr = self.normalize_data(self.rank_df).values
        transformed_arr = self.execute_pca(features_arr)
        self.add_clustering_data(transformed_arr)
        self.set_similar_person(self.rank_df)
        self.set_wca_url_list()
    
    def preprocess_rank_df(self, df):
        person_df = df[df['id'] == self.wca_id]
        country_df = df[df['country'] == self.country]
        return country_df.append(person_df)
        
    def normalize_data(self, df):
        df_norm = df[df.columns[df.dtypes == float]].apply(lambda x : ((x - x.mean())*1/x.std()+0),axis=0)
        return df_norm
    
    def execute_pca(self, feature_arr):
        pca = PCA(n_components = self.pca_dimension)
        transformed_arr = pca.fit_transform(feature_arr) 
        return transformed_arr
    
    def add_clustering_data(self, feature_arr):
        pred_arr = KMeans(n_clusters=self.cluster_num).fit_predict(feature_arr)
        self.rank_df['cluster'] = pred_arr

    def set_similar_person(self, df):
        cluster_id = df[df['id'] == self.wca_id]['cluster'].iloc[0]
        df = df[df['id'] != self.wca_id]
        same_cluster_id_list = df[df['cluster'] == cluster_id]['id'].tolist()
        self.similar_wca_id_list = same_cluster_id_list
    
    def set_wca_url_list(self):
        wca_url_list = []
        for wca_id in self.similar_wca_id_list:
            url = "https://www.worldcubeassociation.org/persons/" + wca_id
            wca_url_list.append(url)
        self.wca_url_list = wca_url_list


    
        







