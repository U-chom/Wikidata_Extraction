import json
import requests
from bs4 import BeautifulSoup
import time
import subprocess
import os
import csv
import pprint
from googletrans import Translator
from elasticsearch import Elasticsearch
import subprocess
import glob
import urllib.request
import re
es = Elasticsearch()
translator = Translator()
mapping = {
    "mappings": {
           "my_type": {
                "properties": {
                    "title": {
                        "type": "keyword"
                    },
                    "ename": {
                        "type": "keyword"
                    },
                    "jname": {
                        "type": "text"
                    }
                }
            }
        }
    }
    

# es.indices.create(index = "wikipropertys", body = mapping)#indexを作る
# es.index(index="wikipropertys",body={"title": "P31","ename": "instance of","jname": "未定"})#要素を入れる
# es.delete(index="wikipropertys",id= 'C1-TDHABEnfRGYNfCx4n')#ドキュメント削除　https://tutorialmore.com/questions-725134.htm
# r = es.index(index="wikipropertys", body={"query": {"match": {"title":"P31"}}})#要素を探す
# print(es.exists(index="wikipropertys",id=r['_id']))
# result = es.get(index="wikipropertys",id=r['_id'])
# result = es.search(index="wikipropertys",body={"query": {"match": {"title":"P495"}}})
# print(result)

def create():
    if es.indices.exists(index = "wikipropertys"):
        print("wikipropertysを更新します．")
        es.indices.delete(index = "wikipropertys")
    es.indices.create(index = "wikipropertys", body = mapping)

def fase0(json_name,Qnum_list,title_list):
    f = open(''+str(json_name)+'.json', 'r',encoding="utf-8")
    json_dict = json.load(f)
    print(json_dict[1])
    for m in json_dict:
        a = m['s'].split("entity/")
        Qnum_list.append(a[1])
        title_list.append(m['sLabel'])
    f.close()

def fase1(title,Qnum,count,titles,json_name): 
    f = open('./'+str(json_name)+'/json/'+str(title)+'.json', 'r',encoding="utf-8")
    json_dict = json.load(f)
    # amount = json_dict["entities"]["Q7851739"]["claims"]["P1082"][0]["mainsnak"]["datavalue"]["value"]["amount"]
    try:
        amountwiki = json_dict["entities"][Qnum]["sitelinks"]["jawiki"]["url"]#変更している．
        with urllib.request.urlopen(amountwiki) as response:
            html = response.read()
            htmlstr = html.decode('utf-8').replace("'",'"')
            stain = htmlstr.split('"')
            for i,url in enumerate(stain):
                if 'wgArticleId' in url:#"wgArticleId":2413586,
                    url = stain[i+1].replace(",","")
                    url = re.sub(":","",url)
                    url = f"https://ja.wikipedia.org/wiki?curid={url}"
                    # print(f"変換url {url}")#理想は{https://ja.wikipedia.org/wiki?curid=1097057}
                    # print("理想 https://ja.wikipedia.org/wiki?curid=1097057")
                    res = es.search(index="neo-wikipedia",body={"query": {"term": {"url.keyword":url}}})#ここも
        hits = res['hits']
        # print(hits)
        first_doc = hits['hits'][0]
        with open("./"+str(json_name)+"/txt/"+str(first_doc['_source']['title'])+".txt","w") as txtfile:
            print(first_doc['_source']['text'],file=txtfile)
        titles.append(title)
        print("記事ファイルを作成しました．")
    except:
        print("日本語記事はありません．")
        with open("./"+str(json_name)+"/errer.txt","a") as errers:
            print(f"{title},{Qnum}",end = '\n',file = errers)
        count += 1
        return "deth_coll"

    amount = json_dict["entities"][Qnum]["claims"]    
    with open("./"+str(json_name)+"/csv/"+str(title)+".csv","w",encoding="utf-8") as T:
        writer = csv.writer(T) 
        for mykey in amount.keys():
            try:
                 amount2 = json_dict["entities"][Qnum]["claims"][mykey][0]["mainsnak"]["datavalue"]["value"]["id"]
            except:
                 writer.writerow([mykey,'NONE'])
            for inf in range(30): 
                try:
                    amount2 = json_dict["entities"][Qnum]["claims"][mykey][inf]["mainsnak"]["datavalue"]["value"]["id"]
                    # print(f"{mykey}=>{amount2}")
                    writer.writerow([mykey,amount2])
                except:
                    break
                    # print(f"{mykey}=>NONE")
    f.close()
    return 

def fase2(title,json_name):
    dxlist = []
    dxlist2 = []
    dary = []
    with open("./"+str(json_name)+"/csv/"+str(title)+".csv",encoding="utf-8") as wiki:
        data = csv.reader(wiki)
        for m in data:
            # print(f"{m[0]},{m[1]}")
            dxlist.append("http://www.wikidata.org/prop/"+str(m[0])+"")
            dxlist2.append("http://www.wikidata.org/wiki/"+str(m[1])+"")
            dary.append(m[0])
        # print(dxlist)
        # print(dxlist2)
    lx = []#property
    for i,da in zip(dxlist,dary):
        # print(da)
        res = es.search(index="wikipropertys",body={"query": {"term": {"title":da}}})#title.keyword
        hits = res['hits']
        if not hits['total'] == 0:
            # print(hits)
            lx.append(hits['hits'][0]['_source']['ename'])
            print("ESは順調です．")
        else:
            r = requests.get(i)
            soup = BeautifulSoup(r.content, "html.parser")
            real_page_tags = soup.find_all('title')
            if len(real_page_tags)==0:
                print("これはないわ")
            # print(real_page_tags)
            j = real_page_tags[0]
            jut = str(j)
            jut = jut.replace("<title>","") 
            jut = jut.replace(" - Wikidata</title>","")   
            lx.append(jut)
            print("ES不調")
            time.sleep(5)

    lx2 = []#value
    for i in dxlist2:
        # print(i)
        if not "NONE" in i:
            r = requests.get(i)
            soup = BeautifulSoup(r.content, "html.parser")
            real_page_tags = soup.find_all('title')
            if len(real_page_tags)==0:
                print("これはないわ")
            # print(real_page_tags)
            j = real_page_tags[0]
            jut = str(j)
            jut = jut.replace("<title>","") 
            jut = jut.replace(" - Wikidata</title>","")   
            lx2.append(jut)
            # print(lx2)
            time.sleep(5)
        else:
            lx2.append("NONE")
            # print(lx2)
    # pickle_dump(lx2,"./a.pickle")
    return lx,lx2
def fase3(title,lx,lx2,json_name):
    with open("./"+str(json_name)+"/PAE/"+str(title)+"PAE.txt","w",encoding="utf8") as AtrE:
        for a,b in zip(lx, lx2): 
            print(f"{a} => {b}",file=AtrE)

def fase3_1(title,lx,lx2,json_name):
    with open("./"+str(json_name)+"/PAE/"+str(title)+"PAE.txt","w",encoding="utf8") as AtrE:
        with open("./"+str(json_name)+"/PAJ/"+str(title)+"PAJ.txt","w",encoding="utf8") as AtrJ:
            with open("./"+str(json_name)+"/csv/"+str(title)+".csv",encoding="utf-8") as wiki:
                data = csv.reader(wiki)
                for wikiid,a,b in zip(data,lx, lx2): 
                    # print(f"wikiid => {wikiid}")
                    print(f"{a} => {b}",file=AtrE)
                    res = es.search(index="wikipropertys",body={"query": {"term": {"title":wikiid[0]}}})
                    hits = res['hits']
                    if not hits['total'] == 0:
                        hits = res['hits']
                        result = hits['hits'][0]
                        if not result['_source']['jname'] == "未定":
                            target1 = result['_source']['jname']
                            print("ESは順調です．")
                        else:
                            try:
                                strJP1 = str(translator.translate(a, dest='ja'))
                                listJP1 = strJP1.split(",")
                                target1 = listJP1[2].replace("text=","") 
                                es.delete(index="wikipropertys",id=result['_id'])
                                es.index(index="wikipropertys",body={"title": wikiid[0],"ename": a,"jname": target1})
                                print(f"{a}の日本語訳を更新しました．")
                            except:
                                target1 = "未定"

                    else:
                        try:
                            strJP1 = str(translator.translate(a, dest='ja'))
                            listJP1 = strJP1.split(",")
                            target1 = listJP1[2].replace("text=","")
                        except:
                            target1 = "未定"

                        try:
                            strJP2 = str(translator.translate(b, dest='ja'))
                            listJP2 = strJP2.split(",")
                            target2 = listJP2[2].replace("text=","")
                        except:
                            target2 = "未定"
                        print(f"{target1} => {target2}",file=AtrJ)
                        es.index(index="wikipropertys",body={"title": wikiid[0],"ename": a,"jname": target1})
                        print(f"新規語{a}を追加しました．日本語訳は{target1}です．")
                        # res = es.search(index="wikipropertys",body={"query": {"term": {"title": wikiid[0]}}})
                        # hits = res['hits']
                        # print(hits)

def rep(text):
    text = text.replace(")","\)")
    text = text.replace("(","\(")
    text = text.replace("（","\（")
    text = text.replace("）","\）")
    # text = text.replace("・","\・")
    return text

def aftercare():
    pathlist = glob.glob("./txt/PAE/*PAE.txt")
    for m in pathlist:
        with open(m,encoding="utf-8") as h:
            paj = h.readlines()
        with open(""+str(m)+"ver.JP","w") as h:
            translator = Translator()
            for n in paj:
                ss = n.split(" => ")
                try:
                    strJP1 = str(translator.translate(ss[0], dest='ja'))
                    listJP1 = strJP1.split(",")
                    target1 = listJP1[2].replace("text=","")
                except:
                    target1 = ss[0]

                try:
                    strJP2 = str(translator.translate(ss[1], dest='ja'))
                    listJP2 = strJP2.split(",")
                    target2 = listJP2[2].replace("text=","")
                except:
                    target2 = ss[1]    
                print(f"{target1} => {target2}",file=h)


# subprocess.run(["curl","-sLH","'Accept: application/json' http://www.wikidata.org/entity/Q8341 | jq .",">","jazz.json"])
def main(title_list,Qnum_list,titles,json_name):
    fase0(json_name,Qnum_list,title_list)
    count = 0
    try:
        subprocess.run(["mkdir",""+str(json_name)+"/txt"])
    except:
        pass
    try:
        subprocess.run(["mkdir",""+str(json_name)+"/csv"])
    except:
        pass
    try:
        subprocess.run(["mkdir",""+str(json_name)+"/json"])
    except:
        pass
    try:
        subprocess.run(["mkdir",""+str(json_name)+"/PAE"])
    except:
        pass
    try:
        subprocess.run(["mkdir",""+str(json_name)+"/PAJ"])
    except:
        pass
    for title,Qnum in zip(title_list,Qnum_list):
        if " " in title:
            title = title.replace(" ","")
        if "/" in title:
            t2 = title.split("/")
            title = t2[1]
        title = rep(title)
        os.system("curl -sLH 'Accept: application/json' http://www.wikidata.org/entity/"+str(Qnum)+" | jq . > ./"+str(json_name)+"/json/"+str(title)+".json")
        print(f"\n\n {title},{Qnum} \n\n")
        try :
            if fase1(title,Qnum,count,titles,json_name) == "deth_coll":
                continue
            print(f"{count}/{len(title_list)} : fase1:END")
    #         lx,lx2 = fase2(title,json_name)
    #         print(lx,lx2)
    #         print(f"{count}/{len(title_list)} : fase2:END")
    #         # fase3(title,lx,lx2)
    #         fase3_1(title,lx,lx2,json_name)
    #         print(f"{count}/{len(title_list)} : fase3:END")
        except:
            with open("./"+str(json_name)+"/errer.txt","a") as errers:
                print(f"{title},{Qnum}",end = '\n',file = errers)
    #     count += 1
    # with open("./"+str(json_name)+"/titlelist.txt","w",encoding="utf-8") as ti:
    #     titlestr = "\n".join(titles)
    #     print(titlestr,file=ti)

def a1(json_name,Qnum_list,title_list):#横暴ver
    fase0(json_name,Qnum_list,title_list)
    titles = title_list
    return titles
def a2(titles):#mainと同時に扱うver
    return titles
def a3(json_name):#ファイル参照
    with open("./"+str(json_name)+"/titlelist.txt",encoding="utf8") as ti:
    # with open("/home/yuki/WorkSpace/data/AAIJournal/Data/Feb_2020/Prototype/ドリンク/titlelist.txt",encoding="utf8") as ti:
        titles = ti.readlines()
    return titles

def wiki_search(key):
    res = es.search(index="neo-wikipedia",body={"query": {"match_phrase": {"text":key}}})
    hits = res['hits']
    first_doc = hits['hits'][0]
    print(hits['total'])
    print(first_doc['_source']['title'])
    print(first_doc['_source']['text'])
    
def search_dump(key):
    query = {"query": {"match_phrase": {"text": key}}}
    res = es.search(index = "neo-wikipedia", body = query)
    print(res['hits']['total'])
    print(json.dumps(res,indent=2,ensure_ascii=False))
    
def searching(keyword,box,sw):
    counter = 1
    size = 10000
    # ドキュメント毎の処理
    def get_doc(counter,hits,box,sw):
        for hit in hits:
            title = hit['_source']['title']
            text = hit['_source']['text']
            # print(f"{counter} : {title} \n {text}")
            print(f"{counter} : {title}")
            if sw == 1:
                box.append(title)
            elif sw == 2:
                box.append(text)
            counter += 1
        return counter

    # response = es.index(index="hoge_index", doc_type="hoge_type", body={"key": "value"})

    # 検索条件
    if sw == 1:
        body={"query": {"match_phrase": {"text": keyword}}}#これでええんや
    elif sw == 2:
        body={"query": {"term": {"title": keyword}}}#これでええんや

    response = es.search( scroll='2m', size=size,
                index='neo-wikipedia',
                body=body)

    sid = response['_scroll_id']
    # print('sid', sid)
    if response['hits']['total'] == 0:
        print(f"{keyword}は存在しません")

    # 検索結果を処理
    counter = get_doc(counter,response['hits']['hits'],box,sw)
    # # スクロールから次の検索結果取得
    response = es.scroll(scroll_id=sid, scroll='10m')
    scroll_size = len(response['hits']['hits'])
    # print( 'scroll_size', scroll_size)

def search_title(keyword,box):
    result = es.search(
            index='neo-wikipedia',#wikipedia
            # body = {"query": {"match": {tsw : keyword}}})#これはやばい
            body = {'query': {'term': {'title.keyword': keyword }}})
            # body={"my_type": {"properties": {"title": "テスト"}}})
    hits = result['hits']
    # print(hits)
    first_doc = hits['hits'][0]#key
    box.append(first_doc['_source']['text'])
    

def main2(json_name,switch,Qnum_list,title_list,titles):
    if switch == 1:
        reject = a1(json_name,Qnum_list,title_list)#横暴ver
    elif switch == 2:
        reject = a2(titles)#mainと同時に扱う
    elif switch == 3:
        reject = a3(json_name)#ファイル参照
    
    title1 = []
    neo_title = []
    # wiki_search("おもちゃ")
    searching(json_name,title1,1)
    for tit in title1:
        for rej in reject:
            if tit == rej:
                break
        neo_title.append(tit)
    neo_title2 = set(neo_title)
    txtlist = []
    for neo in neo_title2:
        search_title(neo,txtlist)
    try:
        subprocess.run(["mkdir",""+(json_name)+"/negative"])
    except:
        pass
    for a,b in zip(neo_title2,txtlist):    
        # with open("/home/yuki/WorkSpace/data/AAIJournal/Data/Feb_2020/Prototype/ドリンク/negative/"+str(a)+".txt","w") as neg:
        with open("./"+str(json_name)+"/negative/"+str(a)+".txt","w") as neg:
            print(b,file = neg)
    print(len(neo_title2),len(txtlist))
 

##----------------------------------------------------------------------------------------------------------------------------
#create()#ESのindex初期化
# json_name = "おもちゃ"#対象のjsonファイルPATH
# title_list = []
# Qnum_list = []
# titles = []
# main()#正例の作成
# main2(json_name,2)#負例の作成
# main2("ドリンク",3,Qnum_list,title_list,titles)

##----------------------------------------------------------------------------------------------------------------------------
def auto():
    all_json = glob.glob("./*.json")
    for json_name in all_json:#タイトルの集合
        json_name = json_name.replace("./","")
        json_name = json_name.replace(".json","")
        subprocess.run(["mkdir",json_name])
        title_list = []
        Qnum_list = []
        titles = []
        main(title_list,Qnum_list,titles,json_name)
        # main2(json_name,2,Qnum_list,title_list,titles)
##----------------------------------------------------------------------------------------------------------------------------
auto()
##----------------------------------------------------------------------------------------------------------------------------



# lx,lx2 = fase2("バターミルク")
# print(lx,lx2)
# aftercare()


# ss = "I am HERO"
# translator = Translator()
# strJP1 = str(translator.translate(ss, dest='ja'))
# print(strJP1)

res = es.search(index="neo-wikipedia",body={"query": {"match_phrase": {"title":"IKKI"}}})
hits = res['hits']
print(hits)
# res = es.search(index="wikipedia",body={"query": {"match_phrase": {"title":"ヤマハ・YZR250"}}})
# res = es.search(index="wikipedia",body={"query": {"term": {"title.keyword":"HondaCBR650R"}}})
# hits = res['hits']
# print(hits)

# DucatiScrambler(2015),Q24286026 