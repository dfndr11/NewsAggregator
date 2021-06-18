#  TODO:   BeautifulSoup + Sentiment, datetime
from GoogleNews import GoogleNews
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

server = 'ANTHONYDESKTOP\MSSQLSERVER01'
database = 'ExcelDocuments'

DB = {'servername': server, 'database': database, 'driver': 'SQL Server Native Client 11.0'}
engine = create_engine('mssql+pyodbc://' + DB['servername'] + '/' + DB['database'] + '?driver=' + DB['driver'])

Max = 4  # Hard cap on the amount of articles to process. 0 = no cap. Can be overriden by value in Excel file

period = "100d"


inputs = pd.read_excel(r'C:\Users\antho\PycharmProjects\NewsAggregator\Input.xlsx')
print(inputs)

catch_all_dataframe = pd.DataFrame()

if str(inputs['Max Articles'][0]) != "nan":
    Max = int(inputs['Max Articles'][0])
    print("Max Overriden: " + str(Max))

if str(inputs['Period'][0]) != "nan":
    period = str(int(inputs['Max Articles'][0])) + "d"
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
            "Sentiment Analysis": []
        }

        if Max > 0:
            results = results[0:Max]  # Caps the results at the Max if needed

        for x in results:
            data["Type"].append(inputs["Type"][index])
            data["Keyword"].append(topic)
            data["Title"].append(x["title"])
            data["Site"].append(x["site"])
            try:
                page = requests.get("http://" + x["link"])
                finalLink = page.url  # Source: https://stackoverflow.com/questions/36070821/how-to-get-redirect-url-using-python-requests
                #if not ("http://" or "https://"  in finalLink):
                    #finalLink = "http://" + finalLink
                data["Link"].append(finalLink)
                #  print(page.content)

                #  soup = BeautifulSoup(page.content, 'html.parser')
                #  print(list(soup.children))

                data["Sentiment Analysis"].append("Placeholder")
            except:
                data["Link"].append("AN EXCEPTION has occurred while accessing this article")
                data["Sentiment Analysis"].append("AN EXCEPTION has occurred while accessing this article")

            articleDatetime = x["datetime"]
            if articleDatetime is not None and str(articleDatetime) != "nan":
                articleDatetime = articleDatetime.strftime('%m/%d/%Y')
                data["Date"].append(articleDatetime)
            else:
                if x["date"] == "Yesterday":
                    data["Date"].append((datetime.now() - timedelta(1)).strftime('%m/%d/%Y'))
                else:
                    data["Date"].append(x["date"])

        temp_dataframe = pd.DataFrame(data)
        print("My dataframe:")
        print(temp_dataframe)

        catch_all_dataframe = catch_all_dataframe.append(temp_dataframe)
        print("Catch all:")
        print(catch_all_dataframe)

        print("---")

ExcelExport = pd.ExcelWriter("CatchAll.xlsx")
catch_all_dataframe.to_excel(ExcelExport)
ExcelExport.save()
catch_all_dataframe.to_sql('ExcelDocuments', con=engine, if_exists='replace', schema='dbo', index=False,
                           chunksize=5000)
