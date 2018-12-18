import json
import numpy
import pickle
import pymongo
import requests
import time
import scipy

from pprint import pprint
from sklearn import linear_model
from collections import OrderedDict
from pymongo import MongoClient
from collections import defaultdict

import common_utils
import mongo_config

client = MongoClient(mongo_config.host, mongo_config.port)
db = client.nba

limit = 0 #1000


def store_games_data(year, season_type):
    year_string = common_utils.construct_year_string(year)
    print("storing games data...")
    games_data = {}
    games = db.games[season_type][year_string].find()
    for game_obj in games:

        home_code = game_obj["home"]
        away_code = game_obj["away"]

        if home_code in common_utils.team_codes_to_bball_ref_codes:
            home_code = common_utils.team_codes_to_bball_ref_codes[home_code]
        if away_code in common_utils.team_codes_to_bball_ref_codes:
            away_code = common_utils.team_codes_to_bball_ref_codes[away_code]

        games_data[game_obj["game_index"]] = {
            "home": home_code,
            "away": away_code
        }
    print("stored games data")
    db.seasons.create_index([
        ('year_string', pymongo.ASCENDING),
    ], unique=True)

    db.seasons.update_one(
        filter = {
            "year_string": year_string
        },
        update = {
            "$set": 
            {
                "{}_games_data".format(season_type): games_data
            }
        }, 
        upsert=True
    )


def convert_lineups_to_tuples(possession, games_data):
    gid = possession["possession_metadata"]["gid"]
    game_info = games_data[gid]
    home_players = possession["home_lineup"]
    away_players = possession["away_lineup"]
    return {
        "home_lineup": [(player, game_info["home"]) for player in home_players],
        "away_lineup": [(player, game_info["away"]) for player in away_players]
    }


def convert_lineups_to_player_team_strings(possession, games_data):
    gid = possession["possession_metadata"]["gid"]
    game_info = games_data[gid]
    # print(game_info)
    home_players = possession["home_lineup"]
    away_players = possession["away_lineup"]
    home_team = game_info["home"]
    away_team = game_info["away"]
    result = {
        "home_lineup": [],
        "away_lineup": []
    }
    for player in home_players:
        if player == "Marcus Paige" and home_team == "IND":
            print(game_info)
            print(possession)
        result["home_lineup"].append("{}_{}".format(player, home_team))
    for player in away_players:
        if player == "Marcus Paige" and away_team == "IND":
            print(game_info)
            print(possession)
        result["away_lineup"].append("{}_{}".format(player, away_team))
    return result


def store_player_and_possession_data_for_matrix(year, season_type):
    player_info = {}
    year_string = common_utils.construct_year_string(year)
    year_as_int = int(year)
    possessions = db.possessions[season_type][year_string].find().limit(limit)
    count = 0
    player_index = 0
    num_possessions = possessions.count()
    games_data = db.seasons.find_one({"year_string": year_string})["{}_games_data".format(season_type)]
    #print(games_data)

    # player info should already be calculated, this is to get index
    if season_type == "playoffs":
        num_players = len(db.seasons.find_one({ "year_string": year_string })["player_info"])

    for possession in possessions:
        #print(possession)
        count += 1
        if count % 20000 == 0:
            print("Poss count {}/{}".format(count, num_possessions))
        lineups_with_team_names = convert_lineups_to_player_team_strings(possession, games_data)
        home_or_away = ["home_lineup", "away_lineup"]
        for lineup_type in home_or_away:
            for player_team in lineups_with_team_names[lineup_type]:
                player_name, team_code = player_team.split("_")
                stub_name = common_utils.player_to_stub_name(player_name)
                #print(player_name, team_code)
                if player_name == "None": 
                    continue
                if player_team not in player_info:
                    if team_code == "WAS" and year_as_int < 1998:
                        team_code = "WSB"

                    # found the player in bball ref database
                    if common_utils.player_exists(stub_name, team_code, year_as_int):
                        player_info[player_team] = {
                            "index": player_index,
                            "possessions": 0,
                            "stub_name" : stub_name
                        }
                        player_index += 1
                    # try to resolve name
                    else:
                        print("{}_{}_{} not found.".format(stub_name, team_code, year_as_int))
                        if season_type == "playoffs":
                            print("\n{}_{}_{} played in playoffs and not in regular season.".format(stub_name, team_code, year_as_int))
                            print("[{}][{}]: {}".format(
                                possession["possession_metadata"]["gid"], 
                                possession["possession_metadata"]["event_num"],
                                possession["possession_metadata"]["message"]
                            ))
                            print("\tHome", lineups_with_team_names["home_lineup"])
                            print("\tAway", lineups_with_team_names["away_lineup"])
                            db.seasons.find_one_and_update(
                                { "year_string": year_string },
                                { "$set" : 
                                    { "player_info.{}".format(player_team): 
                                        {
                                            "index": num_players,
                                            "possessions": 0,
                                            "stub_name": stub_name
                                        }  
                                    } 
                                }
                            )
                            num_players += 1


                # player was already seen in a lineup
                else:
                    player_info[player_team]["possessions"] += 1

    for player_team in player_info:

        possession_number = player_info[player_team]["possessions"]
        player_name, team_code = player_team.split("_")
        #print(player_info[player_team]["stub_name"], team_code)
        db.players.update_one(
            filter = {
                "player_index.name_stub": player_info[player_team]["stub_name"],
                "player_index.team": team_code,                
                "player_index.season": year_as_int
            },
            update = {
                "$set": {
                    "{}_possessions".format(season_type): possession_number
                }
            },
            upsert=True
        )

    if season_type == "regular_season": 
        db.seasons.find_one_and_update(
            { "year_string": year_string },
            { "$set" : { "player_info": player_info  } }
        )
    #pprint(player_info)


def build_matrix(year, season_type="regular_season",prior = "indicator"):

    if prior == "indicator":
        prior_multiplier = 1
    year_string = common_utils.construct_year_string(year)

    print('getting games data for {}'.format(year_string))
    timestamp = time.perf_counter()
    games_data = db.seasons.find_one({"year_string":year_string})["{}_games_data".format(season_type)]
    print("time took {} seconds".format(time.perf_counter()-timestamp))
    print('getting all players list')
    timestamp = time.perf_counter()

    count = 0

    print("time took {} seconds".format(time.perf_counter()-timestamp))
    all_players_dict = db.seasons.find_one({"year_string": year_string})["player_info"]
    print("sample of players from {}".format(year_string))
    for player in all_players_dict:
        print(player)
        count += 1
        if count > 10:
            break
    print('got all players from {}'.format(year_string))
    x_players = []
    y_scores = []
    timestamp = time.perf_counter()
    print('building matrix')
    possessions = db.possessions[season_type][year_string].find().limit(limit)
    possession_index = 0

    num_possessions = possessions.count()
    prev_poss_timestamp = time.perf_counter()

    num_players = len(all_players_dict)
    print("num players: {}".format(num_players))
    player_keys = {}
    for player_team_string in all_players_dict:
        player_keys[player_team_string] = all_players_dict[player_team_string]["index"]
    offense_matrix = scipy.sparse.dok_matrix((num_possessions, num_players), numpy.dtype(float))
    defense_matrix = scipy.sparse.dok_matrix((num_possessions, num_players), numpy.dtype(float))

    for row_num, possession in enumerate(possessions):
        if possession_index % 20000 == 0:
            print("{} / {} possessions added".format(possession_index, num_possessions))
            print("\t{} seconds elapsed".format(time.perf_counter() - prev_poss_timestamp))
            prev_poss_timestamp = time.perf_counter()

        lineups_with_team_names = convert_lineups_to_player_team_strings(possession, games_data)

        try:
            if possession['home_team_is_on_offense']:
                for player in lineups_with_team_names['home_lineup']:
                    if "None" not in player:
                        player_index = player_keys[player]
                        offense_matrix[row_num, player_index] = 1 * prior_multiplier
                for player in lineups_with_team_names['away_lineup']:
                    if "None" not in player:
                        player_index = player_keys[player]
                        defense_matrix[row_num, player_index] = 1 * prior_multiplier
            else:
                for player in lineups_with_team_names['home_lineup']:
                    if "None" not in player:
                        player_index = player_keys[player]
                        defense_matrix[row_num, player_index] = 1 * prior_multiplier
                for player in lineups_with_team_names['away_lineup']:
                    if "None" not in player:
                        player_index = player_keys[player]
                        offense_matrix[row_num, player_index] = 1 * prior_multiplier

        except Exception as e:
            print(e)

        y_scores.append(possession['scoring_margin_update'])

        possession_index += 1



    print('matrix built')
    print("time took {} seconds".format(time.perf_counter()-timestamp))

    print('combining...')
    timestamp = time.perf_counter()

    X = scipy.sparse.hstack((offense_matrix, defense_matrix)).tocsr()
    Y = numpy.array(y_scores)

    print("time took {} seconds".format(time.perf_counter()-timestamp))
    with open("./matrices/{}-X-{}.indicator.pickle".format(year_string, season_type), "wb") as picklefile:
        pickle.dump(X, picklefile)
    with open("./matrices/{}-Y-{}.pickle".format(year_string, season_type),"wb") as picklefile:
        pickle.dump(Y, picklefile)

    print('pickling...')
    timestamp = time.perf_counter()


    print("time took {} seconds".format(time.perf_counter()-timestamp))
    return X, Y
def calculate_rapm(year, X, Y, metric="rapm", season_type="regular_season"):

    year_as_int = int(year)
    year_string = common_utils.construct_year_string(year)
    print('fitting model...')
    timestamp = time.perf_counter()
    multiplier = 1
    if metric == "apm": 
        clf = linear_model.LinearRegression()
        multiplier = 10
    elif metric == "rapm":
        clf = linear_model.Ridge(alpha=2900)
        multiplier = 100
    elif metric == "rapm_enet_cv":
        clf = linear_model.ElasticNetCV(l1_ratio=.9,cv=3)
    elif metric == "rapm_bayes_ridge":
        clf = linear_model.BayesianRidge()
    elif metric == "rapm_cv":
        clf = linear_model.RidgeCV(alphas=list(range(2600,3000,50)), cv=5)
        multiplier = 100
    else:
        raise RuntimeError("{} not recognized".format(metric))

    clf.fit(X, Y)

    if metric == "rapm_enet_cv" or metric == "rapm_cv":
        print("alpha chosen: {}".format(clf.alpha_))
    if metric == "rapm_enet_cv":
        print("l1_ratio chosen: {}".format(clf.l1_ratio_))

    print("time took {} seconds".format(time.perf_counter()-timestamp))
    all_players_dict = db.seasons.find_one({"year_string": year_string})["player_info"]

    print("coefficients", clf.coef_.shape)
    print("num of players", len(all_players_dict))
    rapm = list(zip(all_players_dict, clf.coef_*multiplier, clf.coef_[len(all_players_dict):]*-1*multiplier))


    print(all_players_dict)
    print(len(rapm))


    for player, opm, dpm in rapm:
        player_name, team_code = player.split("_")
        data = {
            'player': player_name,
            'team': team_code,
            'o' + metric: opm,
            'd' + metric: dpm,
            metric: opm + dpm
        }

        stub_name = common_utils.player_to_stub_name(data["player"])
        team_code = data["team"]
        year_as_int = int(year)

        print ("updating {}_{}_{}".format(stub_name, team_code, year_as_int))
        db.players.update_one(
            {
                "player_index.name_stub": stub_name,
                "player_index.team": team_code,
                "player_index.season": year_as_int
            },
            {
                "$set": {
                    "o" + metric + "_" + season_type: data["o" + metric],
                    "d" + metric + "_" + season_type: data["d" + metric],
                    metric + "_" + season_type: data[metric]
                }
            }
        )

    print("Sorted & filtered {} for {}".format(metric, year_string))
    poss_threshold = 1000
    sorted_opm = db.players.find(
        filter = {"player_index.season": year_as_int, "possessions": {"$gt": poss_threshold}},
        projection = {"player": 1, metric + "_" + season_type: 1},
        sort = [(metric + "_" + season_type, -1)]
    )
    rankings_to_print = 50
    for player in sorted_opm:
        if "player" in player:
            print("{:20} {:>10}".format(player["player"], player[metric + "_" + season_type]))
            rankings_to_print += -1
            if rankings_to_print == 0:
                break
    return clf

weird_players = {
    "Melvin Booker",
    "Matt Fish",
    "Gaylon Nickerson",
    "Josh Davis"
}

def deal_with_traded_players(year):
    year_as_int = int(year)
    year_string = common_utils.construct_year_string(year)
    traded_players = db.players.find(
        filter = {
            "player_index.team": "TOT",
            "player_index.season": year_as_int
        }
    )
    for player in traded_players:
        print(player["player"])
        if player["player"] in weird_players:
            continue
        each_team = db.players.find(
            filter = {
                "player_index.name_stub": player["player_index"]["name_stub"],
                "player_index.season": year_as_int,
                "player_index.team": { "$ne": "TOT" }
            }
        )
        
        
        player_update_data = {
            "total_possessions": 0,
            "orapm_weighted_sum": 0,
            "drapm_weighted_sum": 0
        }
        for player_data in each_team:
            print("\t",player_data["player"], player_data["team_id"], player_data["possessions"], player_data["orapm"])
            player_update_data["total_possessions"] += player_data["possessions"]
            # technically possession weights here should be divided by 2 but after the weighted average it won't matter
            player_update_data["orapm_weighted_sum"] += player_data["possessions"] * player_data["orapm"]
            player_update_data["drapm_weighted_sum"] += player_data["possessions"] * player_data["drapm"]
            
        player_update_data["orapm_combined"] = player_update_data["orapm_weighted_sum"] / player_update_data["total_possessions"]
        player_update_data["drapm_combined"] = player_update_data["drapm_weighted_sum"] / player_update_data["total_possessions"]
        player_update_data["rapm_combined"] = player_update_data["orapm_combined"] + player_update_data["drapm_combined"]
        
        pprint(player_update_data)
        
        db.players.update_one(
            filter = {
                "player_index.name_stub": player["player_index"]["name_stub"],
                "player_index.team": "TOT",
                "player_index.season": year_as_int
            },
            update = {
                "$set" : {
                    "possessions": player_update_data["total_possessions"],
                    "orapm": player_update_data["orapm_combined"],
                    "drapm": player_update_data["drapm_combined"],
                    "rapm": player_update_data["rapm_combined"]
                }
            }
        )

def write_rapm_json(year, season_type):
    year_as_int = int(year)
    year_string = common_utils.construct_year_string(year_as_int)
    projection = {
        "player": 1,
        "team_id": 1,
        "{}_possessions".format(season_type): 1,
        "orapm_{}".format(season_type): 1,
        "drapm_{}".format(season_type): 1,
        "rapm_{}".format(season_type): 1
    }

    players = db.players.find(
        filter = {
            "player_index.season": year_as_int
        },
        projection = projection,
        sort = [('rapm_{}'.format(season_type), -1)]
    )

    rapm_json = {
        "data": []
    }
    rank = 1
    for player in players:
        numerical_keys = {
            "orapm_{}".format(season_type),
            "drapm_{}".format(season_type),
            "rapm_{}".format(season_type)
        }

        rapm_sum = 0

        for key in numerical_keys:
            if key in player:
                rapm_sum += player[key]
            else:
                continue
        if rapm_sum == 0:
            continue

        row = [rank]
        for key in projection:
            try:
                if "rapm" in key:
                    row.append(round(player[key], 4))
                else:
                    row.append(player[key])
            except:
                # about a dozen players total that need to be investigated as
                # to why they're edge cases, one thing seems to be Washington Bullets
                # Melvin Booker keeps causing problems man
                # print(player)
                continue
        rank += 1

        if len(row) == 7:
            rapm_json["data"].append(row)

    with open("{}-{}-rapm.json".format(year_string, season_type),"w") as jsonfile:
        json.dump(rapm_json, jsonfile)

if __name__ == "__main__":
    #work_used_to_generate_map_codes()
    # for year in range(2000, 2001):
    #     calculate_rapm(str(year))
    # calculate_rapm(str(2018))
    # for year in range(1997, 2018):
    #     store_games_data(year, "playoffs")
    #     store_player_and_possession_data_for_matrix(year, "playoffs")
    #     X, Y = build_matrix(year, "playoffs")
    #     calculate_rapm(year, X, Y, "rapm", "playoffs")
    for year in range(1997, 2019):
        write_rapm_json(year, "playoffs")
    pass
