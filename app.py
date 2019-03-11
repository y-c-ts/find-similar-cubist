from flask import Flask, request, render_template
from analyze_wca_data import ClusterWCAData
import numpy as np
import pandas as pd
import MySQLdb

EVENT_LIST = ['3x3', '2x2', '4x4', '5x5', '6x6', '7x7', '3BLD', 'FMC', 'OH', 'feet', 'clock', 'mega',
            'pyr', 'sk', 'sq', '4BLD', '5BLD', 'MBLD']
USE_COLUMNS = ['name', 'single', 'average', 'worldRank']

MYSQL_USERNAME = "####"
MYSQL_PASSWORD = "####"
MYSQL_HOST ="####"
MYSQL_DATABASE ="####"

app = Flask(__name__)
@app.route('/', methods=['GET'])
def render_form():
    countries = make_use_countries(pd.read_csv('./data/all_data.csv'))
    return render_template('layout.html',countries = countries)

@app.route('/result', methods=["POST"])
def show_result():
    if request.form['wca_id'] and request.form['country']:
        countries = make_use_countries(pd.read_csv('./data/all_data.csv'))
        cluster_num = int(request.form['cluster_num'])
        country = request.form['country']
        cluster_wca_data_obj = ClusterWCAData('./data/all_data.csv', 14, cluster_num, request.form['wca_id'], country)
        id_list = cluster_wca_data_obj.similar_wca_id_list[:10]
        url_list = cluster_wca_data_obj.wca_url_list[:10]
        cuber_info_list = []
        for i in range(len(id_list)):
            cuber_dict = {}
            cuber_dict['id'] = id_list[i]
            cuber_dict['url'] = url_list[i]
            cuber_info_list.append(cuber_dict)
        return render_template('result.html', wca_id_list = cuber_info_list, own_wca_id = request.form['wca_id'], countries = countries, country=country)
    else:
        return render_template('layout.html')

@app.route('/compare/<string:id_str>')
def compare_result(id_str):
    my_id = id_str.split('_')[0]
    target_id = id_str.split('_')[1]
    my_dict = get_personal_result(my_id)
    target_dict = get_personal_result(target_id)
    return render_template('compare.html', my_dict=my_dict, target_dict=target_dict, my_id=my_id, target_id=target_id )

def connect_database():
    return MySQLdb.connect(
    user = MYSQL_USERNAME ,
    passwd = MYSQL_PASSWORD ,
    host = MYSQL_HOST ,
    db = MYSQL_DATABASE,
    )

def get_personal_result(wca_id):
    conn = connect_database()
    sql =  "SELECT * FROM \
    (SELECT personId, eventId, worldRank, best AS single FROM RanksSingle WHERE personId = '{id}') PersonSingle \
        LEFT OUTER JOIN \
    (SELECT personId, eventId, worldRank, best AS average FROM RanksAverage WHERE personId = '{id}') PersonAverage \
        ON PersonSingle.eventId = PersonAverage.eventId \
    INNER JOIN Events ON PersonSingle.eventId = Events.id".format(id=wca_id)
    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    return rows

def make_use_countries(df):
    grouped = df.groupby('country')
    count_df = pd.DataFrame(grouped['id'].count())
    use_country = count_df[(count_df['id'] > 500) & (count_df['id'] < 5000)].index
    return use_country.tolist()


def make_use_countries(df):
    grouped = df.groupby('country')
    count_df = pd.DataFrame(grouped['id'].count())
    use_country = count_df[(count_df['id'] > 500) & (count_df['id'] < 5000)].index
    return use_country.tolist()


def make_use_countries(df):
    grouped = df.groupby('country')
    count_df = pd.DataFrame(grouped['id'].count())
    use_country = count_df[(count_df['id'] > 500) & (count_df['id'] < 5000)].index
    return use_country.tolist()




if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=True)
