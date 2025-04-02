from flask import Flask, jsonify
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def difference_time(timestamp):
    now = datetime.now().timestamp()
    difference = (now - int(float(timestamp))) / 3600
    if int(float(difference)) <= 0:
        return 'Just Now'
    else:
        return str(int(float(difference))) + (' Hour' if int(float(difference)) == 1 else ' Hours')

def get_info():
    response = {}
    web_response = requests.get("https://trends24.in/france",headers={"user-agent":"Mozilla/5.0"})
    content_bs = BeautifulSoup(web_response.content,'html.parser')
    print(content_bs)
    for div in content_bs.find_all('div',attrs={"class":"list-container"}):
        timestamp = div.find('h3',attrs={'class','title'})
        if timestamp != None:
            timestamp = timestamp['data-timestamp']
            dif = difference_time(timestamp)
            ol = div.find('ol',attrs={"class":"trend-card__list"})
            temp = {'trend':[]}
            for li in ol.find_all('li'):
                tn = li.find('span',attrs={"class":"trend-name"})
                trend_name = tn.find('a',attrs={'class':'trend-link'}).text
                trend_count = tn.find('span',attrs={'class':'tweet-count'})['data-count']
                temp['trend'].append({'name':trend_name,'count':trend_count})
            response[dif] = temp
    return jsonify(response)


if __name__ == '__main__':
    app = Flask(__name__)
    @app.route('/')
    def trends():
        return get_info()
    app.run(debug=True)