import serpapi
import json
import os

client = serpapi.Client(api_key=os.getenv('SERPAPI_KEY'))
params = {
    'engine' : os.getenv('ENGINE'),
    'geo' : 'FR',
    'date' : 'now 24-H',
    'cat' : 0,
    'csv' : 1
}

def get_trends(params):
    search = client.search(params)
    results = search.as_dict()
    return results

def __main__(*args, **kwargs):
    results = get_trends(params)
    print(json.dumps(results, indent=2, ensure_ascii=False))
    
if __name__ == '__main__':
    __main__()