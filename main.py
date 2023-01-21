import datetime
import os
import time
import pandas
import requests
from bs4 import BeautifulSoup as bs
#pandas parse html and scrape the url to their profile page

OUTPUT_FILE = 'BoxScore_.csv'
DONE_LOG = 'log.txt'
BASE_URL = 'https://www.basketball-reference.com'
HEADER = {
    'authority': 'www.basketball-reference.com',
    'method': 'GET',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
}

START_DATE = '' #YYYY-MM-DD format

# df = pandas.read_html('https://www.basketball-reference.com/leagues/NBA_2023_totals.html')

# def getAllPlayersURL():
#     url = 'https://www.basketball-reference.com/leagues/NBA_2023_totals.html'

#     #get the html from the url
#     html = requests.get(url).content

#     #parse the html
#     soup = bs(html, 'html.parser')

#     allPlayersCell = soup.find_all('td', {'data-stat': 'player'})

#     for player in allPlayersCell:
#         yield BASE_URL + player.find('a').get('href').split(".html")[0]

def getSeason():
    url = 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'

    # months = ['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june']
    months = ['january','february', 'march', 'april', 'may', 'june', 'october', 'november', 'december']

    for i in range(2020, 2023):
        for month in months:
            try:
                print(url.format(i, month))
                r = requests.get(url.format(i, month), headers=HEADER, allow_redirects=True)
                # r = requests.get(url.format(i, month), headers=HEADER, proxies={
                #     "http": "http://qtjbcuii-rotate:6ontcavyvek0@p.webshare.io:80/",
                #     "https": "http://qtjbcuii-rotate:6ontcavyvek0@p.webshare.io:80/"
                # })
                if r.status_code == 200:
                    yield url.format(i, month)

                print(r.status_code)
                time.sleep(1.5)
            except Exception as e:
                print(i, e)
                continue


def updateCSV(df: pandas.DataFrame):
    #append csv file
    
    #write header if file is empty
    todayDate = datetime.datetime.now().strftime('%Y-%m-%d')
    if not os.path.exists(OUTPUT_FILE.replace('.csv', f'_{todayDate}' + '.csv')):
        df.to_csv(OUTPUT_FILE.replace('.csv', f'_{todayDate}' + '.csv'), index=False, header=True)
        return

    outputFile = open(OUTPUT_FILE.replace('.csv', f'_{todayDate}' + '.csv'), 'a', newline='', encoding='utf-8')
    df.to_csv(outputFile, index=False, header=False)
    outputFile.close()

# def getStats(url):
#     #scrape game logs within 2020-2023

#     for i in range(20, 23):
#         try:
#             r = requests.get(url + f"/gamelog/{i+2000}", headers=HEADER)
        
            
#             if r.status_code == 429:
#                 print("429 error")
#                 print(r.headers)
#                 time.sleep(60)
#                 continue

#             soup = bs(r.content, 'html.parser')
#             f = open("index.html", "w", encoding="utf-8")
#             f.write(str(soup.prettify()))
#             f.close()
#             logSummary = soup.find('div', {'id': 'div_game_log_summary'})

#             if not logSummary:
#                 continue

#             print("Season: ", i+2000-1, i+2000)

#             dfs = pandas.read_html(str(soup), keep_default_na=True, header=0)

#             for df in dfs:
#                 if "Date" in df.columns:
#                     #rename
#                     df.rename(columns={
#                         'Tm': 'Team', 'Opp': 'Team Against', 
#                         'MP': 'Minutes', 'TRB': 'Rebounds', 
#                         'AST': 'Assists', 'STL': 'Steals',
#                         'BLK': 'Blocks', 'TOV': 'Turnovers', 
#                         'PTS': 'Points', 'Unnamed: 5': 'Venue'
#                     }, inplace=True)

#                     #group by date and drop all rows with 'Date' as value
#                     df = df.groupby('Date').first().reset_index()

#                     #drop all rows with 'Date' as value
#                     df = df[df['Date'] != 'Date']

#                     #add url column
#                     df['URL'] = url + f"/gamelog/{i+2000}"

#                     #add season column
#                     df['Season'] = f"{i+2000-1}-{i+2000}"

#                     print(df)

#                     #Append to csv
#                     updateCSV(df)

#         except requests.exceptions.RequestException as e:
#             print(e)
#             print(r.headers)
#             return
#         except Exception as e:
#             print(e)
#             return

def getBoxScore(soup: bs, url: str):
    #get table

    try:
        table = soup.find('div', {'id': 'div_schedule'}).find('table').find('tbody')
        for row in table.find_all('tr', class_=False):
            boxScore = row.find('td', {'data-stat': 'box_score_text'})
            if boxScore:
                awayTeam = row.find('td', {'data-stat': 'visitor_team_name'}).get_text()
                homeTeam = row.find('td', {'data-stat': 'home_team_name'}).get_text()
                yield BASE_URL + boxScore.find('a').get('href'), awayTeam, homeTeam
    except Exception as e:
        print("Error in [getBoxScore]", url)
        print(e)
        open("error.html", "w", encoding="utf-8").write(str(soup.prettify()))
        return "Error in [getBoxScore]"


def dfHandler(list_dfs: list[pandas.DataFrame]) -> pandas.DataFrame:

    for i, df in enumerate(list_dfs):
        df = df.reset_index(drop=True)
        if "Reserves" in df['Starters'].values:
            new_header = df.iloc[0]
            df = df[1:] #take the data less the header row
            df.columns = new_header #set the header row as the df header

        list_dfs[i] = df

    for i, df in enumerate(list_dfs):
        if "Reserves" in df.columns:
            df["LineUp"] = "Reserves"
            # df.drop("Reserves", axis=1, inplace=True)

            df.rename(columns = {'Reserves':'Name'}, inplace = True)

        if "Starters" in df.columns:
            df['LineUp'] = "Starters"
            df.rename(columns = {'Starters':'Name'}, inplace = True)
            # df.drop("Starters", axis=1, inplace=True)
        
        list_dfs[i] = df
        
    cols = list_dfs[0].columns

    merged_df = pandas.concat(list_dfs)
    merged_df = merged_df.reset_index(drop=True)
    merged_df = merged_df[cols]

    return merged_df

def parseBoxScore(soup: bs, url: str, awayTeam: str, homeTeam: str, season: str, playoff: bool = False):

    # get date
    date = soup.find('div', {'class': 'scorebox_meta'}).find('div')
    
    # parse date
    if date:
        date = datetime.datetime.strptime(date.text, '%H:%M %p, %B %d, %Y')

    # f = open("index.html", "w", encoding="utf-8")
    # f.write(str(soup.prettify()))
    # f.close()

    basicStats = soup.find_all('table', {'class': 'sortable stats_table'})

    teamNames = []

    for table in basicStats:
        if 'game-basic' not in table.get('id'):
            table.decompose()
            continue

        teamName = table.find_previous('h2').get_text().replace(" Basic and Advanced Stats", "")

        if teamName and teamName not in teamNames:
            teamNames.append(teamName)
            # print(teamName)

        # headerRow = table.find('tbody').find_all('tr', class_=True)
        # for row in headerRow:
        #     row.decompose()

        df = pandas.read_html(str(table), keep_default_na=True, header=1)[0]

        #find row contains 'Team Totals' and delete it
        df = df[df['Starters'] != 'Team Totals']


        
        #split dataframe at row has "Reservers" in column

        #index of all rows containing "Reservers" in column Starters
        idx = [0] + df.index[df['Starters'] == "Reserves"].tolist() + [df.shape[0]]
        #list of data frames split at all Reserves indices
        list_of_dfs = [df.iloc[idx[n]:idx[n+1]] for n in range(len(idx)-1)]

        df = dfHandler(list_of_dfs)

        #add date column except the row contain NaN values

        df['Date'] = date

        #add url column
        df['URL'] = url

        #add team column
        df['Team'] = teamName

        #add opponent column
        df['Opponent'] = ''

        #add season
        df['Season'] = season

        if teamName == homeTeam:
            df['Home Team'] = True
            df['Opponent'] = awayTeam

        else:
            df['Home Team'] = False
            df['Opponent'] = homeTeam

        allRows = table.find('tbody').find_all('tr', class_=False)
        # print("All rows: ", len(allRows))

        for row in allRows:

            playerCode = row.find('th', {'data-append-csv': True})
            if playerCode:
                playerCode = playerCode.get('data-append-csv')
            else:
                playerCode = 'N/A'

            #append playerCode to df at row index same as playerCode
            df.at[allRows.index(row), 'Player Code'] = playerCode

        if playoff:
            df['Playoff'] = True
        else:
            df['Playoff'] = False

        updateCSV(df)
            
def done(url: str):
    f = open(DONE_LOG, "a", encoding="utf-8")
    f.write(url + "\n")
    f.close()

def getDone():
    try:
        return open(DONE_LOG, "r", encoding="utf-8").read().split("\n")
    except FileNotFoundError:
        return []
    
def getSeasonYear(soup :bs, playoff: bool = False) -> str:
    """
        Get the season year from the soup
        args:
            soup: bs4 soup
    """

    #get info element
    info = soup.find('div', {'id': 'info'})

    #get h1 element
    h1 = info.find('h1')

    if playoff:
        year = h1.get_text().strip().strip(" NBA Playoffs Schedule and Results")
        return year

    #get year
    year = h1.find('span')

    if year:
        return year.get_text()
    else:
        return ""

def redirect(soup: bs) -> str:
    """
        Check if the page is a redirect page (js)
        args:
            soup: bs4 soup
    """

    #get the first script element
    script = soup.find('script', {'type': 'text/javascript'})

    if script:
        #if script contain window.location.href return the url
        if "window.location.href" in script.get_text():
            return script.get_text().split("window.location.href = \"")[1].split("\"")[0]
        else:
            return ""
    return ""

def process(season: str, playoff: bool = False):


    doneList = getDone()

    #====================================
    #checkpoint to resume from last time
    # checkpointURL = "https://www.basketball-reference.com/leagues/NBA_2020_games-march.html"
    # checkpoint = False
    # bScheckpointURL = "https://www.basketball-reference.com/boxscores/202003040DAL.html"
    # bScheckpoint = False
    # if season != checkpointURL and checkpoint == False:
            # done(season)
            # return
    # elif season == checkpointURL:
    #     checkpoint = True
    
    print("Processing: ", season)
    try:
        r = requests.get(season, headers=HEADER, allow_redirects=True)
        # r = requests.get(season, headers=HEADER, proxies={
        # "http": "http://qtjbcuii-rotate:6ontcavyvek0@p.webshare.io:80/",
        # "https": "http://qtjbcuii-rotate:6ontcavyvek0@p.webshare.io:80/"
        # }, allow_redirects=True)

        if r.status_code != 200:
            print("Error: ", r.status_code, season)
            return

        soup = bs(r.content, 'html.parser')
        
        # f = open("index.html", "w", encoding="utf-8")
        # f.write(soup.prettify())
        # f.close()

        redirectURL = redirect(soup)
        if redirectURL != "":
            print("Redirecting to: ", BASE_URL + redirectURL)
            return process(BASE_URL + redirectURL)

        seasonYear = ""
        if playoff:
            seasonYear = getSeasonYear(soup, True)
        else:
            seasonYear = getSeasonYear(soup)


        for url, awayTeam, homeTeam in getBoxScore(soup, season):
            try:

                if url in doneList:
                    continue

                #checkpoint to resume from last time
                # if url != bScheckpointURL and bScheckpoint == False:
                #     done(url)
                #     continue
                # elif url == bScheckpointURL:
                #     bScheckpoint = True
                
                print(url)

                # r = requests.get(url, headers=HEADER)
                r = requests.get(url, headers=HEADER, proxies={
                    "http": "http://qtjbcuii-rotate:6ontcavyvek0@p.webshare.io:80/",
                    "https": "http://qtjbcuii-rotate:6ontcavyvek0@p.webshare.io:80/"
                })

                soup = bs(r.content, 'html.parser')
                parseBoxScore(soup, url, awayTeam, homeTeam, seasonYear, playoff)

                done(url)

                time.sleep(1.5)
            except KeyboardInterrupt:
                print("Keyboard Interrupt")
                break
            except Exception as e:
                print("Error in [process]: ", e, url)
                continue

    except Exception as e:
        print("Error in [process]: ", e, season)
        return

def playoffURL(season: str) -> list:
    """
        Playoffs
    """
    #Example: 2023
    url = 'https://www.basketball-reference.com/playoffs/NBA_{}_games.html'.format(season)
    return url

def seasonURL(date: datetime.datetime) -> list:
    """
        Regular Seasons
    """
    #Example: 2023
    url = 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
    urls = []
    #===========================
    #generate urls until current date
    #example: input: 10-01-2020
    #return -> all months until current date (01-20-2023)
    #===========================
    today = datetime.datetime.now()
    todayMonth = today.month
    
    for year in range(date.year, today.year + 1):
        if year == today.year:
            #if current year, only generate until current month
            for month in months[:todayMonth]:
                urls.append(url.format(year, month))
        elif year == date.year:
            #if start year, only generate from start month
            for month in months[months.index(date.strftime('%B').lower()):]:
                urls.append(url.format(year, month))
        else:
            #generate all months
            for month in months:
                urls.append(url.format(year, month))
    
    return urls

def processFromDate(season: str, date: datetime.datetime, playoff: bool = False):
    """
        Process from a specific date
    """
    print("Processing from date: ", date.strftime('%Y-%m-%d'), "\nSeason: ", season, "\nPlayoff: ", playoff)

    if playoff:
        return process(playoffURL(season), True)

    # startURL = 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'.format(season, date.strftime('%B').lower())

    #Get all urls from start date
    # urls = seasonURL(season)[seasonURL(season).index(startURL):]
    urls = seasonURL(season)
    return urls

if __name__ == "__main__":
    
    # urls = open("urlList.txt", "r", encoding="utf-8").read().split("\n")
    
    # print(urls)

    # doneList = getDone()
    # for season in getSeason():
        #write to file
        # open("urlList.txt", "a", encoding="utf-8").write(season + "\n")
        # pass

    # print(seasonURL("2023"))

    # urls = seasonURL("2020")

    # playoffURLs = playoffURL("2020")
    # print(playoffURLs)


    
    # for season in playoffURLs[:1]:
    #     try:
    #         process(season, True)
    #         done(season)
    #     except KeyboardInterrupt:
    #         break
    #     except Exception as e:
    #         print("Error in [main]: ", e)
        
    # Test processFromDate()
    print("=====================================================")
    print("Example: 10-01-2022 will scrape season 2022-2023 from October 2022")
    date = datetime.datetime.strptime(input("Input start date (MM-DD-YYYY): "), '%m-%d-%Y')
    choice = input("Playoff? (y/n): ")
    if choice.lower().strip() == 'y':
        print(processFromDate(str(date.year), date, True))
    elif choice.lower().strip() == 'n':
        urls = processFromDate(str(date.year + 1), date)
        from multiprocessing.pool import Pool
        try:
            with Pool(5) as p:
                p.map(process, urls)
        except KeyboardInterrupt:
            print("Keyboard Interrupt")
            exit()
        except Exception as e:
            print("Error in [main]: ", e)

    else:
        print("Invalid Choice")
        exit()

    #===========================
    # print(seasonURL(date))