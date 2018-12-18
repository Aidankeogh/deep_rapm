import csv
import json
import pymongo
import os
import tarfile

from pymongo import MongoClient
from pprint import pprint

import mongo_config
client = MongoClient(mongo_config.host, mongo_config.port)
db = client.nba

import common_utils


def construct_year_string(year):
    return str(int(year)-1) + '-' + str(year)[2:]

def unarchive_data(year):
    year_string = construct_year_string(year)
    tar_filename = year_string + ".tar.gz"
    tar = tarfile.open(tar_filename)
    tar.extractall()
    tar.close()

def unarchive_files():
    # Would be better: Scan directory for tar gz files to unpack
    for year in range(1998, 2018+1):
        year = str(year)
        year_string = construct_year_string(year)
        tar_filename = year_string + ".tar.gz"
        tar = tarfile.open(tar_filename)
        tar.extractall()
        tar.close()

def import_players_into_mongo(year):
    year_string = construct_year_string(year)
    print('creating index')
    db.players.create_index([
        ('player_index.name_stub', pymongo.ASCENDING),
        ('player_index.season', pymongo.ASCENDING),
        ('player_index.team', pymongo.ASCENDING)
    ], unique=True)

    pprint(db.players.index_information())
    print('index created (or already existed)')

    with open("{}/players_advanced_{}.json".format(year_string, year), "r") as playerfile:
        player_json = json.load(playerfile)

        headers = player_json.pop(0)
        headers.pop(1)
        headers.append(("Season", "season", None))

        for index, player_row in enumerate(player_json):
            player_row.append(year)
            mongo_row = {}
            for index, element in enumerate(player_row):
                try: 
                    mongo_row[headers[index][1]] = float(player_row[index])
                except ValueError as e:
                    mongo_row[headers[index][1]] = player_row[index]
            mongo_row["player"] = mongo_row["player"].replace("*", "")
            stub_name = common_utils.player_to_stub_name(mongo_row["player"])
            
            print("import {}_{}_{}".format(stub_name, mongo_row["team_id"], year))
            
            db.players.update_one(
                {
                    'player_index.name_stub': stub_name,
                    'player_index.team': mongo_row["team_id"].strip(),
                    'player_index.season': year
                },
                {'$set': mongo_row}, 
                True
            )

def import_teams_into_mongo(year):
    year_string = construct_year_string(year)
    print('creating index')
    db.teams.create_index([
        ('team_index.team_id', pymongo.ASCENDING),
        ('team_index.season', pymongo.ASCENDING)
    ], unique=True)
    print('index created (or already existed)')

    with open("{}/teams_{}.json".format(year_string, year), "r") as teamfile:
        team_json = json.load(teamfile)
        headers = team_json.pop(0)
        headers.append(("Season", "season", None))
        print(len(headers))
        for row_index, team_row in enumerate(team_json):
            team_row.append(year)
            mongo_row = {}
            for ele_index, element in enumerate(team_row):
                try:
                    mongo_row[headers[ele_index][1]] = float(team_row[ele_index])
                except ValueError as e:
                    print(e)
                    mongo_row[headers[ele_index][1]] = team_row[ele_index]
            print("{}_{}".format(mongo_row["season"], mongo_row["team_name"]))
            db.teams.update_one(
                {
                    'team_index.team_id': mongo_row["team_code"],
                    'team_index.season': year
                },
                {'$set': mongo_row}, 
                True
            )

def import_game_info_into_mongo(year, season_type="regular_season"):
    year_string = construct_year_string(year)
    with open("{}/games_{}_{}.json".format(year_string, season_type, year), "r") as jsonfile:
        games_json = json.load(jsonfile)

        for game_obj in games_json:
            if game_obj["away"] in common_utils.team_codes_to_bball_ref_codes:
                away_team = common_utils.team_codes_to_bball_ref_codes[game_obj["away"]]
            else:
                away_team = game_obj["away"]

            if game_obj["home"] in common_utils.team_codes_to_bball_ref_codes:
                home_team = common_utils.team_codes_to_bball_ref_codes[game_obj["home"]]
            else:
                home_team = game_obj["home"]

            if home_team == "CHA" and int(year) > 2014:
                home_team = "CHO"
            if away_team == "CHA" and int(year) > 2014:
                away_team = "CHO"

            db.games[season_type][year_string].update_one(
                { "game_index": game_obj["game_id"]},
                { "$set": {
                        "away": away_team,
                        "home": home_team,
                        "date": game_obj["date"]
                    }
                })
            #print("Updated info for game_id {}".format(game_obj["game_id"]))


corrections = {
    "0029600021" : [
        # event id, event field index, event field substution
        (78, 20, 1489),
        (78, 21, "Lionel Simmons"),
        (302, 20, 1489),
        (302, 21, "Lionel Simmons"),
        (449, 13, 1489),
        (449, 14, "Lionel Simmons")
    ],
    "0029600301" : [
        (358, 21, "Melvin Booker"),
        (358, 20, 511)
    ]
}

def manual_corrections(game_id, pbp_json):
    for correction in corrections[game_id]:
        event_id, field_id, substition = correction 
        pbp_json[event_id][field_id] = substitution
    return pbp_json

def import_games_pbp_into_mongo(year, season_type="regular_season"):

    year_string = construct_year_string(year)
    print('creating database index')
    db.games[year_string].create_index([
        ('game_index', pymongo.ASCENDING)
    ], unique=True)
    print('database index created (or already existed)')
    print('Importing game_ids... (only showing every 100)')
    with open("{}/games_{}_{}.json".format(year_string, season_type, year), "r") as gamefile:
        game_json = json.load(gamefile)

        for index, game in enumerate(game_json):
            game_id = game["game_id"]
            with open("{}/{}/{}.json".format(year_string, season_type, game_id), "r") as pbpfile:
                pbp_json = json.load(pbpfile)

                if game_id in corrections:
                    pbp_json = manual_corrections(game_id, pbp_json)

                db.games[season_type][year_string].update_one(
                    {'game_index': game_id},
                    {'$set': {
                        "pbp": pbp_json
                       }
                    }, 
                    True)
            # print out every 100th game inserted as measure of progress
            if index % 100 == 0:
                print("Inserted game_id {}".format(game_id))


def get_bball_ref_team_codes(year):
    year_string = construct_year_string(year)
    filename = "{}/teams_{}.json".format(year_string, year)
    if not os.path.exists(filename):
        return set()
    with open(filename,"r") as jsonfile:
        data = json.load(jsonfile)
        result = set()
        for index, ele in enumerate(data):
            if index == 0: 
                continue
            result.add(ele[0])
        return result


def get_espn_team_codes(year):
    year_string = construct_year_string(year)
    filename = "{}/player_rpms_{}_rpms.csv".format(year_string, year)
    if not os.path.exists(filename):
        return set()
    with open(filename,"r") as csvfile:
        data = csv.reader(csvfile)
        result = set()
        for index, ele in enumerate(data):
            if index == 0: 
                continue
            team_codes = ele[2].split("/")
            for code in team_codes:
                result.add(code)
        return result

def get_stats_nba_team_codes(year):
    year_string = construct_year_string(year)
    filename = "{}/games_regular_season_{}.json".format(year_string, year)
    if not os.path.exists(filename):
        return set()
    with open(filename,"r") as jsonfile:
        data = json.load(jsonfile)
        result = set()
        for index, ele in enumerate(data):
            result.add(ele["away"])
            result.add(ele["home"])
        return result

def work_used_to_generate_map_codes():
    bball_ref_codes = set()
    stats_nba_codes = set()
    espn_codes = set()
    for year in range(1997, 2018+1):
        bball_ref_codes |= get_bball_ref_team_codes(year)
        stats_nba_codes |= get_stats_nba_team_codes(year)
        espn_codes |= get_espn_team_codes(year)

    # for key in map_codes_to_bball_ref_codes:
    #     if key in stats_nba_codes:
    #         stats_nba_codes.remove(key)
    #         stats_nba_codes.add(map_codes_to_bball_ref_codes[key])
    #     if key in espn_codes:
    #         espn_codes.remove(key)
    #         espn_codes.add(map_codes_to_bball_ref_codes[key])

    print(espn_codes - bball_ref_codes)

    print(stats_nba_codes - bball_ref_codes)

    print("bballref:", sorted(list(bball_ref_codes)))
    print("stats.nba:", sorted(list(stats_nba_codes)))
    print("espn:", sorted(list(espn_codes)))

def add_rpm_to_player_table(year):
    if int(year) < 2014:
        print("No RPM data available prior to 2013-14 season")
        return
    year_string = construct_year_string(year)
    rpm_filename = "{}/player_rpms_{}_rpms.csv".format(year_string, year)
    with open(rpm_filename,"r") as csvfile:
        data = csv.reader(csvfile)
        for index, ele in enumerate(data):
            # skip header row
            if index == 0: 
                continue
            team_codes = ele[2].split("/")
            player_name = ele[0]
            if not player_name:
                continue
            player_stub_name = common_utils.player_to_stub_name(player_name)
            for code in team_codes:
                if code in common_utils.team_codes_to_bball_ref_codes:
                    code = common_utils.team_codes_to_bball_ref_codes[code]

                player_name = common_utils.nba_com_player_name_to_bball_ref_player_name(player_name, code, year)
                player_stub_name = common_utils.player_to_stub_name(player_name)
                rpm_update = {
                    "orpm": float(ele[5]),
                    "drpm": float(ele[6]),
                    "rpm": float(ele[7])
                }
                db.players.update_one(
                    {
                        'player_index.name_stub': player_stub_name,
                        'player_index.team': code,
                        'player_index.season': year
                    },
                    { '$set': rpm_update} , 
                    False
                )
                print("Added RPM for {}".format(player_name))

def work_to_resolve_player_names():
    pass

if __name__ == "__main__":
    #work_used_to_generate_map_codes()
    for year in range(1997, 2018):
        import_games_pbp_into_mongo(year, "playoffs")
        import_game_info_into_mongo(year, "playoffs")

        # #unarchive_data(year)
        # import_players_into_mongo(year)
        # add_rpm_to_player_table(year)
        # import_teams_into_mongo(year)


