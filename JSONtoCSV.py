import json
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        addr=data["result"][k]["addresses"]
        for a in addr:
            print(data['result'][k]['id'], end='')
            print (",",a,end='')
            print(',',data['result'][k]['category'], end='')
            print("")