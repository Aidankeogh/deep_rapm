import json
import operator
import os
import pandas
import requests
import tarfile
import time

from bs4 import BeautifulSoup
from pprint import pprint

# Season types retrieved from error message of this API Call: 
# https://stats.nba.com/stats/leaguegamelog?Season=2015-16&SeasonType=Pre%20season&Sorter=DATE&Direction=DESC&PlayerOrTeam=T&LeagueID=00
# The field SeasonType must match the regular expression '^(Regular Season)|(Pre Season)|(Playoffs)|(All Star)|(All-Star)$'.
season_types = {
    "Regular Season": "regular_season",
    "Playoffs": "playoffs",
    "Pre Season": "preseason",
    "All Star": "allstar"
}

user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36"
headers = {'User-Agent': user_agent}

def construct_pbp_url(game_id):
    return "http://stats.nba.com/stats/playbyplayv2/?GameID={}&StartPeriod=1&EndPeriod=10".format(game_id)

def construct_year_string(year):
    return str(int(year)-1) + '-' + str(year)[2:]

def construct_schedule_url(year='2017', season_type='Regular Season'):
    year_string = construct_year_string(year)
    return "https://stats.nba.com/stats/leaguegamelog?Season={}&SeasonType={}&Sorter=DATE&Direction=DESC&PlayerOrTeam=T&LeagueID=00".format(year_string, season_type)

def fetch_game_ids(year='2017', season_type='Regular Season'):
    url = construct_schedule_url(year, season_type)
    print(url)
    print(requests.get(url, headers=headers))
    data = requests.get(url, headers=headers).json()

    games_seen = set()
    games = []
    for game in data['resultSets'][0]['rowSet']:
        # Looks like "Away @ Home"
        game_id = game[4]
        game_date = game[5]
        matchup_string = game[6]
        # api has entry for each team, so 2 for each game, with the matchup id having "vs." instead of "@".
        # we ignore them
        if "@" in matchup_string and game_id not in games_seen:
            games_seen.add(game_id)
            away_team, _, home_team = matchup_string.split()
            games.append({
                "away": away_team,
                "home": home_team,
                "game_id": game_id,
                "date": game_date
            })

    return sorted(games, key=operator.itemgetter("game_id"))

def fetch_pbp(game_id):
    url = construct_pbp_url(game_id)
    r = requests.get(url, headers=headers)
    data = r.json()
    pbp = data['resultSets'][0]['rowSet']
    return pbp

def get_player_stats(year, page_type="advanced"): 
    if page_type not in set(["totals", "per_poss", "advanced", "per_minute"]):
        raise RuntimeError("Invalid Basketball Ref Player Page type")

    player_data_url = "https://www.basketball-reference.com/leagues/NBA_{}_{}.html".format(year, page_type)
    response = requests.get(player_data_url)
    soup = BeautifulSoup(response.content, "html.parser")
    
    table = soup.find("table")
    table_head = table.find("thead")
    header_items = table_head.find_all("tr")[0].find_all("th")
    
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")
    header = []
    for ele in header_items:
        header_tuple = (ele.get("aria-label"), ele.get("data-stat"), ele.get("data-tip"))
        header.append(header_tuple)
    header.insert(0,("Basketball-Reference Player ID", "bball_ref_player_id", "Player reference ID on basketball reference"))
    
    data = [header]

    for row in rows:
        cols = []
        page_row_items = row.find_all("td")
        if page_row_items:
            for ele in page_row_items:
                if ele.has_attr("data-append-csv"):
                    bball_ref_player_id = ele.get("data-append-csv")
                cols.append(ele.text.strip())
            cols.insert(0,bball_ref_player_id)
            data.append(cols) # Get rid of empty values


    return data

def scrape_bball_ref(year="2017"):
    url = "https://www.basketball-reference.com/leagues/NBA_{}.html".format(year)
    response = requests.get(url)
    return response

def get_team_stats(year):
    response = scrape_bball_ref(year)
    html_string = str(response.content)
    misc_stats_table_location = html_string.find("all_misc_stats")
    html_string = html_string[misc_stats_table_location:]
    html_string = html_string.replace("<!--", "",1).replace("-->","",1)
    soup = BeautifulSoup(html_string, "html.parser")
    tables = soup.find_all("table")
    table = soup.find("table")
    table_head = table.find("thead")
    header_items = table_head.find_all("tr")[1].find_all("th")
    
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")
    header = [("Team Code", "team_code", "Abbreviation for Team Name")]

    for ele in header_items:
        header_tuple = (ele.get("aria-label"), ele.get("data-stat"), ele.get("data-tip"))
        # skip this
        if ele.get("data-stat") == "ranker":
            continue
        header.append(header_tuple)
    
    header.append(("Playoffs", "made_playoffs", "Designates whether team made the postseason"))
    data = [header]

    for row in rows:
        cols = []
        page_row_items = row.find_all("td")
        if page_row_items:
            made_playoffs = None
            for index, ele in enumerate(page_row_items):
                if ele.get("data-stat") == "team_name":
                    a_tag = ele.find_all("a")[0]
                    href = a_tag.get("href")
                    team_code = href.split("/")[2]
                    cols.append(team_code)
                # skip this
                if ele.get("data-stat") == "ranker":
                    continue
                # team name is at index 0, (initial rank column doesn't appear to be td?)
                # bball ref puts asterisks after each name if the made the playoffs
                # we put this info  in a separate column
                if index == 0:
                    if "*" in ele.text:
                        made_playoffs = 1
                    else:
                        made_playoffs = 0
                cols.append(ele.text.strip("* "))

            cols.append(made_playoffs)
        data.append(cols) 
    return data

def collect_all_season_data(year):
    year = str(year)
    year_string = construct_year_string(year)
    season_data_path = os.path.join(os.getcwd(), year_string)
    # if directory does not exist create it
    if not os.path.isdir(season_data_path):
        os.mkdir(season_data_path)

    for season_type in season_types:
        subseason_data_path = os.path.join(season_data_path, season_types[season_type])
        if not os.path.isdir(subseason_data_path):
            os.mkdir(subseason_data_path)

        # get play by plays
        # fetch game ids, save each json
        game_data_filepath = os.path.join(season_data_path, "games_{}_{}.json".format(season_types[season_type], year))
        if not os.path.exists(game_data_filepath):
            game_data = fetch_game_ids(year, season_type)
            with open(game_data_filepath,"w") as game_file:
                print("Write {}".format(game_data_filepath))
                json.dump(game_data, game_file)
        else:
            print("Found {}".format(game_data_filepath))

        game_data = json.load(open(game_data_filepath, "r"))

        for game in game_data:
            game_id = game["game_id"]
            file_path = os.path.join(subseason_data_path, game_id) + ".json"
            if not os.path.exists(file_path):
                pbp_json = fetch_pbp(game_id)
                with open(file_path,"w") as json_file:
                    print("Write {}".format(file_path))
                    json.dump(pbp_json, json_file)
            else:
                print("Found {}".format(file_path))

    # get team stats
    team_data_filepath = os.path.join(season_data_path, "teams_{}.json".format(year))
    if not os.path.exists(team_data_filepath):
        team_data = get_team_stats(str(year))
        with open(team_data_filepath,"w") as team_file:
            print("Write {}".format(team_data_filepath))
            json.dump(team_data, team_file)
    else:
        print("Found {}".format(team_data_filepath))

    # get player stats
    # per possession, per minute, we can calculate on our own
    for page_type in ["totals", "advanced"]:
        player_data_filepath = os.path.join(season_data_path, "players_{}_{}.json".format(page_type, year))
        if not os.path.exists(player_data_filepath):
            player_data = get_player_stats(year, page_type)
            with open(player_data_filepath,"w") as player_file:
                print("Write {}".format(player_data_filepath))
                json.dump(player_data, player_file)
        else:
            print("Found {}".format(player_data_filepath))
    
    if int(year) >= 2014:
        scrape_and_save_rpm(year)

    # tar the directories up
    tar_directory(year_string)

def tar_directory(dirname):
    archive_name = dirname + ".tar.gz"
    #if not os.path.exists(archive_name):
    tar = tarfile.open(archive_name, "w:gz")
    tar.add(dirname)
    tar.close()
    print("Created {}".format(archive_name))

def get_number_of_pages(year):
    rpm_url = "http://www.espn.com/nba/statistics/rpm/_/year/{}/page/1".format(year)
    response = requests.get(rpm_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    page_number_div = soup.find_all("div", class_="page-numbers")[0]
    number_of_pages = int(page_number_div.text.split()[2])
    return number_of_pages

def get_header(df):
    return df.index[0]    
    
def scrape_and_save_rpm(year):
    year_string = construct_year_string(year)
    number_of_rpm_pages = get_number_of_pages(year)
    rpm_df = pandas.DataFrame()
    tables = []
    for i in range(number_of_rpm_pages):
        url_page_num = i + 1
        rpm_url = "http://www.espn.com/nba/statistics/rpm/_/year/{}/page/{}".format(year, url_page_num)
        table = pandas.read_html(rpm_url)[0]
        tables.append(table)
        print("Scraped ESPN's {} RPM Page {}/{}".format(year_string, url_page_num, number_of_rpm_pages))
    
    header = get_header(tables[0])
    for table in tables:
        table.drop(table.index[0], inplace=True)
    rpm_df = pandas.concat(tables)
    print("Table concatenated")

    rpm_df["name"] = rpm_df[1].str.split(",").str.get(0)
    rpm_df["pos"] = rpm_df[1].str.split(",").str.get(1).str.strip()
    rpm_df.columns = ["rank", "throwaway", "team", "games", "mpg", "orpm", "drpm", "rpm", "wins", "name", "pos"]
    rpm_df = rpm_df.drop(["rank","throwaway"], axis=1)
    columns = ["name", "pos", "team", "games", "mpg", "orpm", "drpm", "rpm", "wins"]
    rpm_df = rpm_df[columns]
    rpm_df["rpm"] = pandas.to_numeric(rpm_df["rpm"])

    rpm_df.sort_values(["rpm"], ascending=False,inplace=True)
    rpm_df.to_csv("{}/player_rpms_{}_rpms.csv".format(year_string, year), index=False)
    print("Saved {}/player_rpms_{}.csv".format(year_string, year))

def get_team_codes(site):
    site_start_years = {
        "espn": 2014,
        "bball-ref": 1997,
        "stats.nba": 1997
    }

    current_year = 2018

    for year in range(site_start_years[site], current_year + 1):
        print(year)

    codes = set()

def get_espn_team_codes(year):
    number_of_rpm_pages = get_number_of_pages(year)
    df = pandas.DataFrame()
    tables = []
    for i in range(number_of_rpm_pages):
        url_page_num = i + 1
        rpm_url = "http://www.espn.com/nba/statistics/rpm/_/year/{}/page/{}".format(year, url_page_num)
        table = pandas.read_html(rpm_url)[0]
        tables.append(table)
        print("Scraped page {}".format(url_page_num))

if __name__ == "__main__":
    # for year in range(1997, 2018+1):
    #     collect_all_season_data(year)
    year = 2018
    collect_all_season_data(year)
    # year_string = construct_year_string(year)
    # season_data_path = os.path.join(os.getcwd(), year_string)

    # for page_type in ["totals", "advanced"]:
    #     player_data_filepath = os.path.join(season_data_path, "players_{}_{}.json".format(page_type, year))
    #     if not os.path.exists(player_data_filepath):
    #         player_data = get_player_stats(year, page_type)
    #         with open(player_data_filepath,"w") as player_file:
    #             print("Write {}".format(player_data_filepath))
    #             json.dump(player_data, player_file)
    #     else:
    #         print("Found {}".format(player_data_filepath))
