#es6用
import elasticsearch
from elasticsearch import Elasticsearch, helpers
import os
import glob
import json

es = Elasticsearch()

mapping = {
        "properties": {
            "title": {
                "type": "keyword"
            },
            "url":{
                "type": "keyword"
            },
            "text": {
                "type": "text",
                "analyzer": "jpn-index",
                "search_analyzer": "jpn-search"
        }
    }
}
  

# es.indices.create(index='wikipedia2', body = mapping)
print(es.indices.exists(index="neo-wikipedia"))
# es.indices.delete('my_index1')
# print(es.get(index="wikipedia",id='-hMkEHABlYU_cnbBMmsk'))

def put_data(num,title,url,doc):
    body = {"title": title, "url": url, "text": doc}
    es.index(index="neo-wikipedia",id=num,body=body)
    # es.index(index="wikipedia",body=body)

def Neo_wikipedia():
    with open("/home/yuki/WorkSpace/data/testdatas/wikipediaTool/wikipedia2020.txt",encoding="utf-8") as wiki:
        wikiline = wiki.readline()
        text = []
        count = 0
        wikiid = 0
        # print(wikiline)
        while wikiline:
            if wikiline == "\n":
                wikiline = wiki.readline()
                continue
            if "</doc>" in wikiline:
                doc = ''.join(text)
                # print(doc)
                res = es.search(index="neo-wikipedia",body={"query": {"term": {"title.keyword":title}}})
                hits = res['hits']
                print(hits['total'])
                if not hits['total'] == 0:
                    print(f"{title}は重複しています．")
                    first_doc = hits['hits'][0]#key
                    print('ヒット数 : %s' % hits['total'])
                    print('タイトル : %s' % first_doc['_source']['title'])
                    if not first_doc['_source']['title'] == title:
                        print("嘘でした，追加します．")
                        put_data(wikiid,title,url,doc)
                else:
                    print("新しい記事を追加します．")
                    print(f"追加される記事 => {title}")
                    put_data(wikiid,title,url,doc)
                # break
                text = []
                count = 3
                wikiid += 1
            if count == 2:
                text.append(wikiline)
            if count == 1:
                # print(f"\n {wikiline} \n")
                title = wikiline.replace("\n","")
                count = 2
            if "<doc " in wikiline:
                wikisplit = wikiline.split(" ")
                url = wikisplit[2]
                url = url.replace('url=','')
                url = url.replace('"','')
                # print(url)
                count = 1
            wikiline = wiki.readline()

def search_title(keyword):
    result = es.search(
            index='neo-wikipedia',#wikipedia
            # body = {"query": {"match": {tsw : keyword}}})#これはやばい
            body = {'query': {'term': {'title.keyword': keyword }}})
            # body={"my_type": {"properties": {"title": "テスト"}}})
    hits = result['hits']
    # print(hits)
    first_doc = hits['hits'][0]#key
    print('ヒット数 : %s' % hits['total'])
    print('ID : %s' % first_doc['_id'])
    print('タイトル : %s' % first_doc['_source']['title'])
    print('url : %s' % first_doc['_source']['url'])
    print('テキスト : %s' % first_doc['_source']['text'])

    hitslist = []
    for h in hits['hits']:
        htitle = h['_source']['title']
        if not htitle in hitslist:
            hitslist.append(h)
    print('ヒットタイトル数 : %s' % len(hitslist))


def searcher(keyword):
    result = es.search(
            index='neo-wikipedia',#wikipedia
            # body = {"query": {"match": {tsw : keyword}}})#これはやばい
            body = {"query": {"match_phrase": {"text" : keyword}}})
            # body={"my_type": {"properties": {"title": "テスト"}}})
    hits = result['hits']
    # print(hits)
    first_doc = hits['hits'][0]#key
    print('ヒット数 : %s' % hits['total'])
    print('ID : %s' % first_doc['_id'])
    print('タイトル : %s' % first_doc['_source']['title'])
    print('テキスト : %s' % first_doc['_source']['text'])

    hitslist = []
    for h in hits['hits']:
        htitle = h['_source']['title']
        if not htitle in hitslist:
            hitslist.append(h)
    print('ヒットタイトル数 : %s' % len(hitslist))

def searcher2(keyword):
    counter = 1
    size = 10000

    # ドキュメント毎の処理
    def get_doc(counter,hits):
        for hit in hits:
            title = hit['_source']['title']
            text = hit['_source']['text']
            print(f"{counter} : {title} \n {text}")
            counter += 1
        return counter

    # response = es.index(index="hoge_index", doc_type="hoge_type", body={"key": "value"})

    # 検索条件
    response = es.search( scroll='2m', size=size,
                index='neo-wikipedia',
                # body={"mappings": {"my_type": {"properties": {"title": "テスト"}}}})
                body={"query": {"match_phrase": {"text":keyword}}})

    sid = response['_scroll_id']
    print('sid', sid)
    print( 'total', response['hits']['total'] )

    # 検索結果を処理
    counter = get_doc(counter,response['hits']['hits'])
    # # スクロールから次の検索結果取得
    response = es.scroll(scroll_id=sid, scroll='10m')
    scroll_size = len(response['hits']['hits'])
    print( 'scroll_size', scroll_size)

def search_dump():
    query = {"query": {"match": {"text": ""}}}
    res = es.search(index = "neo-wikipedia", body = query)
    print(json.dumps(res,indent=2,ensure_ascii=False))

def create_jp_index():#日本語用インデックスの登録
    # インデックス作成用JSONの定義
    create_index = {
        "settings": {
            "analysis": {
                "filter": {
                    "synonyms_filter": { # 同義語フィルターの定義
                        "type": "synonym",
                        "synonyms": [ #同義語リストの定義 (今は空の状態)
                            ]
                    }
                },
                "tokenizer": {
                    "kuromoji_w_dic": { # カスタム形態素解析の定義
                    "type": "kuromoji_tokenizer", # kromoji_tokenizerをベースにする
                        # ユーザー辞書としてmy_dic.dicを追加  
                        "user_dictionary": "my_dic.dic" 
                    }
                },
                "analyzer": {
                    "jpn-search": { # 検索用アナライザの定義
                        "type": "custom",
                        "char_filter": [
                            "icu_normalizer", # 文字単位の正規化
                            "kuromoji_iteration_mark" # 繰り返し文字の正規化
                        ],
                        "tokenizer": "kuromoji_w_dic", # 辞書付きkuromoji形態素解析
                        "filter": [
                            "synonyms_filter", # 同義語展開
                            # "kuromoji_baseform", # 活用語の原型化
                            # "kuromoji_part_of_speech", # 不要品詞の除去
                            # "ja_stop", #不要単語の除去
                            "kuromoji_number", # 数字の正規化
                            "kuromoji_stemmer" #長音の正規化
                        ]
                    },
                    "jpn-index": { # インデックス生成用アナライザの定義
                        "type": "custom",
                        "char_filter": [
                            "icu_normalizer", # 文字単位の正規化
                            "kuromoji_iteration_mark" # 繰り返し文字の正規化
                        ],
                        "tokenizer": "kuromoji_w_dic", # 辞書付きkuromoji形態素解析
                        "filter": [
                            # "kuromoji_baseform", # 活用語の原型化
                            # "kuromoji_part_of_speech", # 不要品詞の除去
                            # "ja_stop", #不要単語の除去
                            "kuromoji_number", # 数字の正規化
                            "kuromoji_stemmer" #長音の正規化
                        ]
                    }
                }
            }
        },
        "properties": {
            "title": {
                "type": "keyword"
            },
            "url":{
                "type": "keyword"
            },
            "text": {
                "type": "text",
                "analyzer": "jpn-index",
                "search_analyzer": "jpn-search"
        }
    }
    }

    # 日本語用インデックス名の定義
    jp_index = 'neo-wikipedia'

    # 同じ名前のインデックスがすでにあれば削除する
    if es.indices.exists(index = jp_index):
        print(f"{jp_index}を更新します．")
        es.indices.delete(index = jp_index)

    # インデックス jp_doc の生成
    es.indices.create(index = jp_index, body = create_index)

def analyse_jp_text(text):
    body = {"analyzer": "jpn-search", "text": text}
    ret = es.indices.analyze(index = "neo-wikipedia", body = body)
    tokens = ret['tokens']
    tokens2 = [token['token'] for token in tokens]
    return tokens2


# create_jp_index() #形態素解析用indexの生成
# Neo_wikipedia()#indexにdocを追加
search_title("バターミルク")
# searcher("街の医者・神山治郎") #10個だけ調べもの(0:title,1:text)
# searcher2("メディアミックス") #すべて調べもの
# search3()
# print(analyse_jp_text('ビームサーベル'))# analyse_jp_test 関数のテスト

#参考文献
# https://blog.imind.jp/entry/2019/03/08/185935

# es.indices.create(index = "wikipropertys", body = mapping)#indexを作る
# es.index(index="wikipropertys",body={"title": "P31","ename": "instance of","jname": "未定"})#要素を入れる
# es.delete(index="wikipropertys",id= 'C1-TDHABEnfRGYNfCx4n')#ドキュメント削除　https://tutorialmore.com/questions-725134.htm
# r = es.index(index="wikipropertys", body={"query": {"match": {"title":"P31"}}})#要素を探す
# print(es.exists(index="wikipropertys",id=r['_id']))
# result = es.get(index="wikipropertys",id=r['_id'])
# result = es.search(index="wikipropertys",body={"query": {"match": {"title":"P31"}}})
# print(result)
