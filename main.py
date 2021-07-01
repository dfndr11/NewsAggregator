from GoogleNews import GoogleNews
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import requests
from google.cloud import language
from urllib.request import urlopen
from bs4 import BeautifulSoup
import traceback
import json

server = 'ANTHONYDESKTOP\MSSQLSERVER01' # CHANGE ME
database = 'ExcelDocuments' # CHANGE ME

DB = {'servername': server, 'database': database, 'driver': 'SQL Server Native Client 11.0'}
engine = create_engine('mssql+pyodbc://' + DB['servername'] + '/' + DB['database'] + '?driver=' + DB['driver'])

client = language.LanguageServiceClient.from_service_account_json(r'C:\Users\antho\PycharmProjects\NewsAggregator\googleinfo.json') # CHANGE ME

Max = 5  # Default cap on the amount of articles to process. 0 = no cap. Can be overridden by value in Excel file
period = "100d"  # Default period for the timeframe articles should be processed from. Can be overridden by value in Excel file




def analyze_text_sentiment(text):
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)

    response = client.analyze_sentiment(document=document)

    sentiment = response.document_sentiment
    results = dict(
        text=text,
        score=f"{sentiment.score:.1%}",
        magnitude=f"{sentiment.magnitude:.1%}",
    )
    for k, v in results.items():
        print(f"{k:10}: {v}")
    result_json = response.__class__.to_json(response)
    result_dict = json.loads(result_json)
    to_return = [str(results["score"]), result_dict["sentences"]]
    return to_return




def classify_text(text):
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)

    response = client.classify_text(document=document)

    results = dict(
        content=response.categories,
    )
    return results




def listToString(s):
    # initialize an empty string
    str1 = ""

    # traverse in the string
    for ele in s:
        str1 += ele + " "

        # return string
    return str1

def get_plain_text(link):
    y = 0
    print(str(link))
    url = str(link)
    html = urlopen(url).read()
    soup = BeautifulSoup(html, features="html.parser")

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())


    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)


    text_to_return = text.splitlines()

    while y < len(text_to_return) - 1:
        x = text_to_return[y]
        words = x.split()
        if len(words) < 40:  # Change me!
            text_to_return.remove(x)
            y = y - 1
        y = y + 1

    foo = listToString(text_to_return)


    foo = foo.split()

    baa = 0
    jee = 0
    ya = False

    for bool in foo:
        print("the currect word is "+ str(bool) + "the current condition for title is " + str(ya) + "the number of cap so far is " + str(jee))
        bool += " "
        if bool[0].isupper():
            jee += 1
            if jee > 10:
                ya = True

        elif ya:
            print("adding .")
            foo[baa-2] = foo[baa-2] + "."
            ya = False
            jee = 0
        baa += 1



    print(foo)
    foo = listToString(foo)
    hehe = False
    for c in foo:
        if c == "." and not hehe:
            hehe = True
            c += " "
            print("addded space into "+ c )
        elif hehe and c == ".":
            c= ""
            hehe = False
        elif hehe and not c == ".":
            hehe = False

    return foo




inputs = pd.read_excel(r'C:\Users\antho\PycharmProjects\NewsAggregator\Input.xlsx')  # CHANGE ME
print(inputs)

catch_all_dataframe = pd.DataFrame()
catch_all_errors = pd.DataFrame()
catch_all_sentences = pd.DataFrame()
catch_all_bodies = pd.DataFrame()

if str(inputs['Max Articles'][0]) != "nan":
    Max = int(inputs['Max Articles'][0])
    print("Max Overriden: " + str(Max))

if str(inputs['Period'][0]) != "nan":
    period = str(int(inputs['Period'][0])) + "d"
    print("Period Overriden: " + period)

for index in inputs.index:
    keyword = str(inputs["Keyword"][index])
    list_of_keywords = [keyword]
    if str(inputs['Additional Keywords'][index]) != "nan":
        additional_keywords = str(inputs["Additional Keywords"][index])
        list_of_keywords = [keyword]
        list_of_keywords = list_of_keywords + additional_keywords.split(",")
    print(list_of_keywords)
    for i in range(len(list_of_keywords)):
        print(i)
        if i == 0:
            topic = (list_of_keywords[i]).strip()
        else:
            topic = (list_of_keywords[0]).strip() + " " + (list_of_keywords[i]).strip()
        print(topic)

        googlenews = GoogleNews(period=period)
        googlenews.get_news(topic)
        results = googlenews.results()
        print(results)

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
            "Value": []
        }

        bodies_dict = {
            "Link": [],
            "Title": [],
            "Body": [],
        }

        if Max > 0:
            results = results[0:Max]  # Caps the results at the Max if needed

        for x in results:
            data["Type"].append(inputs["Type"][index])
            data["Keyword"].append(topic)
            data["Title"].append(x["title"])
            data["Site"].append(x["site"])
            try:
                text_sent = (analyze_text_sentiment(x["title"]))[0]
                print("title success")
                print((analyze_text_sentiment(x["title"]))[0])
                data["Title Sentiment"].append(text_sent)
            except:
                print("title exception")
                data["Title Sentiment"] = "999.9%"
            try:
                page = requests.get("http://" + x["link"])
                finalLink = page.url  # Source: https://stackoverflow.com/questions/36070821/how-to-get-redirect-url-using-python-requests
                #if not ("http://" or "https://"  in finalLink):
                    #finalLink = "http://" + finalLink
                data["Link"].append(finalLink)
                #  print(page.content)

                #  soup = BeautifulSoup(page.content, 'html.parser')
                #  print(list(soup.children))
                try:
                    plain_text = str(get_plain_text(finalLink))
                    sentiment_results = analyze_text_sentiment(plain_text)
                    sentiment = sentiment_results[0]
                    print(sentiment)
                    data["Article Sentiment"].append(str(sentiment))
                    # Bodies Excel
                    bodies_dict["Link"].append(finalLink)
                    bodies_dict["Title"].append(x["title"])
                    bodies_dict["Body"].append(plain_text)

                    # Sentence Sentiment
                    try:
                        sentences_list = sentiment_results[1]
                        content_list = []
                        score_list = []
                        for a in range(len(sentences_list)):
                            content_list.append(sentences_list[a]["text"]["content"])
                            score_list.append(float(sentences_list[a]["sentiment"]["score"]))
                        sorted_score_list = sorted(score_list)
                        amt = int(int(len(score_list)) / 2)
                        if amt > 5:
                            amt = 5
                        taken_i = []
                        if amt > 0:
                            print("amt", amt)
                            for a in range(amt):
                                a = len(score_list) - a - 1
                                for y in range(len(score_list)):
                                    if sorted_score_list[a] == score_list[y] and y not in taken_i:
                                        taken_i.append(y)
                                        sentences_dict["Link"].append(finalLink)
                                        sentences_dict["Title"].append(x["title"])
                                        sentences_dict["Sentence"].append(content_list[y])
                                        sentences_dict["Value"].append(score_list[y])
                                        break
                            for a in range(amt):
                                for y in range(len(score_list)):
                                    if sorted_score_list[a] == score_list[y] and y not in taken_i:
                                        taken_i.append(y)
                                        sentences_dict["Link"].append(finalLink)
                                        sentences_dict["Title"].append(x["title"])
                                        sentences_dict["Sentence"].append(content_list[y])
                                        sentences_dict["Value"].append(score_list[y])
                                        break
                        else:
                            print("Not enough sentences?")
                    except:
                        print(traceback.format_exc())

                except:
                    print("---EXCEPTION:S---")
                    data["Article Sentiment"].append("999.9%")
                try:
                    plain_text = str(get_plain_text(finalLink))
                    result = classify_text(plain_text)
                    print("result:")
                    print(type(result))
                    print(str(result))
                    print(result)
                    print(len(result))
                    if len(result) > 1:
                        categories = (str(result["content"][1]).split('"'))[1]
                        print(categories)
                        category_list = (categories.split("/"))
                        print(category_list)
                        category = category_list[len(category_list) - 1]
                        print(category)
                        data["Category"].append(category)
                    elif len(result) > 0:
                        categories = (str(result["content"][0]).split('"'))[1]
                        print(categories)
                        category_list = (categories.split("/"))
                        print(category_list)
                        category = category_list[len(category_list) - 1]
                        print(category)
                        data["Category"].append(category)
                    else:
                        data["Category"].append("An exception has occurred.")
                except:
                    data["Category"].append("An exception has occurred.")
            except:
                print(traceback.format_exc())
                print("---EXCEPTION:A---")
                data["Link"].append("An exception has occurred.")
                data["Article Sentiment"].append("999.9%")
                data["Category"].append("An exception has occurred.")
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

            print("SENTT:", sentences_dict)

        print("data:", data)
        temp_dataframe = pd.DataFrame(data)
        print("My dataframe:")
        print(temp_dataframe)

        catch_all_dataframe = catch_all_dataframe.append(temp_dataframe)
        print("Catch all:")
        print(catch_all_dataframe)

        temp_errors = pd.DataFrame(errors)
        catch_all_errors = catch_all_errors.append(temp_errors)

        temp_sentences = pd.DataFrame(sentences_dict)
        print("temp_sentences,", temp_sentences)
        catch_all_sentences = catch_all_sentences.append(temp_sentences)
        print("catch_sentences", catch_all_sentences)

        temp_bodies = pd.DataFrame(bodies_dict)
        print("temp_bodies,", temp_bodies)
        catch_all_bodies = catch_all_bodies.append(temp_bodies)
        print("catch_bodies", catch_all_bodies)

        print("---")

print(catch_all_errors)
ExcelExport = pd.ExcelWriter("CatchAll.xlsx")
catch_all_dataframe.to_excel(ExcelExport)
ExcelExport.save()
catch_all_dataframe.to_sql('ExcelDocuments', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)

ExcelExport2 = pd.ExcelWriter("ErrorLog.xlsx")
catch_all_errors.to_excel(ExcelExport2)
ExcelExport2.save()
catch_all_errors.to_sql('ErrorLog', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)

print("cas,", catch_all_sentences)
ExcelExport3 = pd.ExcelWriter("ArticleSentences.xlsx")
catch_all_sentences.to_excel(ExcelExport3)
ExcelExport3.save()
catch_all_sentences.to_sql('ArticleSentences', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)

print("bod,", catch_all_bodies)
ExcelExport4 = pd.ExcelWriter("ArticleBodies.xlsx")
catch_all_bodies.to_excel(ExcelExport4)
ExcelExport4.save()
catch_all_bodies.to_sql('ArticleBodies', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)