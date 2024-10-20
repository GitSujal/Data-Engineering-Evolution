import requests
request_base_url = r"https://www.yelp.com.au/gql/batch"
payload = [{"operationName":"GetBusinessReviewFeed","variables":{"encBizId":"30UX2wFjo_jK82LQVbW1-g",
                                                                 "reviewsPerPage":30,
                                                                 "selectedReviewEncId":"",
                                                                 "hasSelectedReview":False,
                                                                 "sortBy":"RELEVANCE_DESC",
                                                                 "languageCode":"en","ratings":[5],
                                                                 "queryText":"",
                                                                 "isSearching":False,
                                                                 "after":"eyJ2ZXJzaW9uIjoxLCJ0eXBlIjoib2Zmc2V0Iiwib2Zmc2V0Ijo5fQ==",
                                                                 "isTranslating":False,
                                                                 "translateLanguageCode":"en",
                                                                 "reactionsSourceFlow":"businessPageReviewSection",
                                                                 "guv":"2337E94512339811",
                                                                 "minConfidenceLevel":"HIGH_CONFIDENCE",
                                                                 "highlightType":"",
                                                                 "highlightIdentifier":"",
                                                                 "isHighlighting":False},
                                                                 "extensions":{"operationType":"query",
                                                                               "documentId":"ef51f33d1b0eccc958dddbf6cde15739c48b34637a00ebe316441031d4bf7681"}}]
response = requests.post(request_base_url,
                        json=payload,
                        headers={'User-Agent': 'Mozilla/5.0'}
                        )
print(response.json())


data = response.json()[0]['data']
count = 0
for edge in data['business']['reviews']['edges']:
    print(edge['node']['text']['full'])
    print(edge['node']['rating'])
    count +=1

print(count)