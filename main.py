from GoogleNews import GoogleNews
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from google.cloud import language
from newspaper import Article
import pandas as pd
import requests
import traceback
import json


server = 'ANTHONYDESKTOP\MSSQLSERVER01'  # CHANGE ME
database = 'ExcelDocuments'  # CHANGE ME
client = language.LanguageServiceClient.from_service_account_json(r'C:\Users\antho\PycharmProjects\NewsAggregator\googleinfo.json')  # CHANGE ME
inputs = pd.read_excel(r'C:\Users\antho\PycharmProjects\NewsAggregator\Input.xlsx')  # CHANGE ME


Max = 5  # Default cap on the amount of articles to process. 0 = no cap. Can be overridden by value in Excel file
period = "100d"  # Default period for the timeframe articles should be processed from. Can be overridden by value in Excel file


DB = {'servername': server, 'database': database, 'driver': 'SQL Server Native Client 11.0'}
engine = create_engine('mssql+pyodbc://' + DB['servername'] + '/' + DB['database'] + '?driver=' + DB['driver'])

# ----------------------------------------------------------------------------------------------------------------------

def analyze_text_sentiment(text):
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)
    response = client.analyze_sentiment(document=document)
    analyzed_sentiment = response.document_sentiment
    analyzed_results = dict(
        text=text,
        score=f"{analyzed_sentiment.score:.1%}",
        magnitude=f"{analyzed_sentiment.magnitude:.1%}",
    )
    result_json = response.__class__.to_json(response)
    result_dict = json.loads(result_json)
    to_return = [str(analyzed_results["score"]), result_dict["sentences"], str(analyzed_results["magnitude"])]  # 0: score, 1: dict of sentences, 2: magnitude
    return to_return


def classify_text(text):
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)
    response = client.classify_text(document=document)
    classified_results = dict(
        content=response.categories,
    )
    return classified_results


def get_plain_text(url):
    article = Article(url)
    article.download()
    article.parse()
    return str(article.text)

# ----------------------------------------------------------------------------------------------------------------------
# Automated Sequence


print("Inputs", inputs)

catch_all_dataframe = pd.DataFrame()
catch_all_errors = pd.DataFrame()
catch_all_sentences = pd.DataFrame()
catch_all_bodies = pd.DataFrame()

if str(inputs['Max Articles'][0]) != "nan":  # Check if a maximum override exists in Input.xlsx
    Max = int(inputs['Max Articles'][0])
    print("Max Overridden: " + str(Max))

if str(inputs['Period'][0]) != "nan":  # Check if a period override exists in Input.xlsx
    period = str(int(inputs['Period'][0])) + "d"
    print("Period Overridden: " + period)

for index in inputs.index:
    keyword = str(inputs["Keyword"][index])
    list_of_keywords = [keyword]
    if str(inputs['Additional Keywords'][index]) != "nan":
        additional_keywords = str(inputs["Additional Keywords"][index])
        list_of_keywords = [keyword]
        list_of_keywords = list_of_keywords + additional_keywords.split(",")
    print("List of keywords", list_of_keywords)
    for i in range(len(list_of_keywords)):
        if i == 0:
            topic = (list_of_keywords[i]).strip()
        else:
            topic = (list_of_keywords[0]).strip() + " " + (list_of_keywords[i]).strip()
        print("i, topic: ", i, topic)

        googlenews = GoogleNews(period=period)
        googlenews.get_news(topic)
        results = googlenews.results()

        data = {
            "Type": [],
            "Keyword": [],
            "Title": [],
            "Site": [],
            "Link": [],
            "Date": [],
            "Category": [],
            "Title Sentiment": [],
            "Article Sentiment": [],
            "Article Magnitude": [],
            "Timestamp Generated": []
        }

        errors = {
            "Title": [],
            "Site": [],
            "Link": [],
            "Timestamp Generated": []
        }

        sentences_dict = {
            "Link": [],
            "Title": [],
            "Sentence": [],
            "Sentiment": [],
            "Magnitude": [],
        }

        bodies_dict = {
            "Link": [],
            "Title": [],
            "Body": [],
            "Sentiment": [],
            "Magnitude": [],
        }

        if Max > 0:
            results = results[0:Max]  # Caps the results at the Max if needed

        for x in results:
            data["Type"].append(inputs["Type"][index])
            data["Keyword"].append(topic)
            data["Title"].append(x["title"])
            data["Site"].append(x["site"])
            bodies_dict["Title"].append(x["title"])
            print("Title: ", x["title"])
            print("Google link: ", x["link"])
            try:
                text_sent = (analyze_text_sentiment(x["title"]))[0]
                print("Title success. Title sentiment:")
                print((analyze_text_sentiment(x["title"]))[0])
                data["Title Sentiment"].append(text_sent)
            except:
                print(traceback.format_exc())
                print("Title exception")
                data["Title Sentiment"] = "999.9%"
            try:
                print("topic:", topic)
                print("Attempting to access link...")
                page = requests.get("http://" + x["link"])
                finalLink = page.url
                data["Link"].append(finalLink)
                bodies_dict["Link"].append(finalLink)
                print("Link fully accessed successfully...")
                try:
                    plain_text = str(get_plain_text(finalLink))
                    sentiment_results = analyze_text_sentiment(plain_text)
                    sentiment = sentiment_results[0]
                    magnitude = sentiment_results[2]
                    print("Article sentiment and magnitude:", sentiment, magnitude)
                    data["Article Sentiment"].append(str(sentiment))
                    data["Article Magnitude"].append(str(magnitude))
                    # Bodies Excel
                    bodies_dict["Body"].append(plain_text)
                    bodies_dict["Sentiment"].append(str(sentiment))
                    bodies_dict["Magnitude"].append(str(magnitude))
                    # Sentence Sentiment
                    try:
                        sentences_list = sentiment_results[1]
                        content_list = []
                        score_list = []
                        magnitude_list = []
                        for a in range(len(sentences_list)):
                            content_list.append(sentences_list[a]["text"]["content"])
                            score_list.append(float(sentences_list[a]["sentiment"]["score"]))
                            magnitude_list.append(float(sentences_list[a]["sentiment"]["magnitude"]))
                        sorted_score_list = sorted(score_list)
                        amt = int(len(score_list))
                        taken_i = []
                        if amt > 0:
                            print("Amt of sentences: ", amt)
                            for a in range(amt):
                                a = len(score_list) - a - 1
                                for y in range(len(score_list)):
                                    if sorted_score_list[a] == score_list[y] and y not in taken_i:
                                        taken_i.append(y)
                                        sentences_dict["Link"].append(finalLink)
                                        sentences_dict["Title"].append(x["title"])
                                        sentences_dict["Sentence"].append(content_list[y])
                                        sentences_dict["Sentiment"].append(score_list[y])
                                        sentences_dict["Magnitude"].append(magnitude_list[y])
                                        break
                        else:
                            print("Not enough sentences?")
                    except:
                        print(traceback.format_exc())
                except:
                    print("Link unable to be accessed...")
                    print(traceback.format_exc())
                    print("---EXCEPTION while performing sentiment analysis / getting article text---")
                    data["Article Sentiment"].append("999.9%")
                    data["Article Magnitude"].append("999.9%")
                    bodies_dict["Body"].append("An exception has occurred.")
                    bodies_dict["Sentiment"].append("999.9%")
                    bodies_dict["Magnitude"].append("999.9%")
                try:
                    plain_text = str(get_plain_text(finalLink))
                    result = classify_text(plain_text)
                    if len(result) > 1:
                        categories = (str(result["content"][1]).split('"'))[1]
                        category_list = (categories.split("/"))
                        category = category_list[len(category_list) - 1]
                        data["Category"].append(category)
                    elif len(result) > 0:
                        categories = (str(result["content"][0]).split('"'))[1]
                        category_list = (categories.split("/"))
                        category = category_list[len(category_list) - 1]
                        data["Category"].append(category)
                    else:
                        data["Category"].append("An exception has occurred.")
                except:
                    print(traceback.format_exc())
                    data["Category"].append("An exception has occurred.")
            except:
                print(traceback.format_exc())
                print("---EXCEPTION while accessing the article---")
                data["Link"].append("An exception has occurred.")
                bodies_dict["Link"].append("An exception has occurred.")
                data["Article Sentiment"].append("999.9%")
                data["Article Magnitude"].append("999.9%")
                data["Category"].append("An exception has occurred.")
                bodies_dict["Sentiment"].append("999.9%")
                bodies_dict["Magnitude"].append("999.9%")
                errors["Title"].append(x["title"])
                errors["Site"].append(x["site"])
                errors["Link"].append(x["link"])
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y_%H:%M:%S")
                errors["Timestamp Generated"].append(dt_string)
            articleDatetime = x["datetime"]
            if articleDatetime is not None and str(articleDatetime) != "nan":
                articleDatetime = articleDatetime.strftime('%m/%d/%Y')
                data["Date"].append(articleDatetime)
            else:
                if x["date"] == "Yesterday":
                    data["Date"].append((datetime.now() - timedelta(1)).strftime('%m/%d/%Y'))
                elif "minutes" in x["date"] or "hours" in x["date"]:
                    data["Date"].append(datetime.now())
                else:
                    data["Date"].append(x["date"])
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y_%H:%M:%S")
            data["Timestamp Generated"].append(dt_string)
            print("Sentences Dict:", sentences_dict)
        print("Data:", data)
        temp_dataframe = pd.DataFrame(data)

        catch_all_dataframe = catch_all_dataframe.append(temp_dataframe)

        print("Errors:", errors)
        temp_errors = pd.DataFrame(errors)
        catch_all_errors = catch_all_errors.append(temp_errors)

        """print("Sentences:", sentences_dict)
        temp_sentences = pd.DataFrame(sentences_dict)
        catch_all_sentences = catch_all_sentences.append(temp_sentences)"""

        print("Bodies:", bodies_dict)
        temp_bodies = pd.DataFrame(bodies_dict)
        catch_all_bodies = catch_all_bodies.append(temp_bodies)

        print("- - - - -")

ExcelExport = pd.ExcelWriter("CatchAll.xlsx")
catch_all_dataframe.to_excel(ExcelExport)
ExcelExport.save()
catch_all_dataframe.to_sql('ExcelDocuments', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)

print("Catch all errors: ", catch_all_errors)
ExcelExport2 = pd.ExcelWriter("ErrorLog.xlsx")
catch_all_errors.to_excel(ExcelExport2)
ExcelExport2.save()
catch_all_errors.to_sql('ErrorLog', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)

"""print("Catch All Sentences,", catch_all_sentences)
ExcelExport3 = pd.ExcelWriter("ArticleSentences.xlsx")
catch_all_sentences.to_excel(ExcelExport3)
ExcelExport3.save()
catch_all_sentences.to_sql('ArticleSentences', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)"""

print("Catch All Bodies,", catch_all_bodies)
ExcelExport4 = pd.ExcelWriter("ArticleBodies.xlsx")
catch_all_bodies.to_excel(ExcelExport4)
ExcelExport4.save()
catch_all_bodies.to_sql('ArticleBodies', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)