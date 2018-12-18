import csv
import time

import numpy
from scipy import stats

import mongo_config
import pymongo
from pymongo import MongoClient

client = MongoClient(mongo_config.host, mongo_config.port)
db = client.nba

from pprint import pprint

import common_utils

players_without_possessions = set()


"""
For each team in a given season S
    Calculate for S-0, S-1, S-2, and S-3
        Possession weighted average for WS/48, PER, BPM, RAPM, RPM (if available)
            Find the R^2 correlation with wins, SRS
"""


def calculate_weighted_average(year):
    year_as_int = int(year)
    metrics_averages = {
        "per": 15.0,
        "bpm": 0.0, 
        "ws_per_48":0.100, 
        "rapm": 0.0,
        "apm": 0.0
    }

    if int(year) >= 2014:
        metrics_averages["rpm"] = 0.0

    year_as_int = int(year)
    year_string = common_utils.construct_year_string(year)
    teams = db.teams.find({"season": year})
    for team in teams:
        team_code = team["team_code"]
        # get the players to loop through
        players = db.players.find(
            {
                "player_index.season": year_as_int, 
                "player_index.team": team_code
            }
        )
        players = list(players)
        if len(players) == 0 and team_code in common_utils.team_renames:
            players = db.players.find({
                "player_index.season": year_as_int, 
                "player_index.team": common_utils.team_renames[team_code]
            })
            players = list(players)

        if len(players) == 0:
            print("could not find {}_{}".format(year_to_evaluate,team["team_index"]["team_id"]))
            continue

        for year_step in range(4): 
            timestamp = time.perf_counter()

            year_to_evaluate = year - year_step
            total_possessions = 0
            weighted_average = 0

            for metric in metrics_averages:
                print("{} for {} {}\n".format(metric, year_to_evaluate, team_code))
                if year_to_evaluate < 1997:
                    print("no data available for prior to 1997")
                    continue
                for player in players:
                    # original measure was 250 minutes
                    if "possessions" in player and "player" in player:
                        stub_name = common_utils.player_to_stub_name(player["player"])
                        prev_season_player_lookup = db.players.find({
                            "player_index.season": year_to_evaluate,
                            "player_index.name_stub": stub_name
                        })

                        if not player_exists(stub_name, year_to_evaluate):
                            if player_minus_jr_exists(stub_name, year_to_evaluate):
                                stub_name = stub_name[:len(stub_name)-2]
                                print("substracted suffix, found {}".format(stub_name))

                            if stub_name in common_utils.nba_com_stubs_to_bball_ref_stubs:
                                stub_name = common_utils.nba_com_stubs_to_bball_ref_stubs[stub_name]

                        # if player still doesn't exist, then idk
                        if not player_exists(stub_name, year_to_evaluate):
                            print("{}_{} not found in scraped bball_ref players page".format(stub_name, year_to_evaluate))
                        else: 
                            prev_season_player_lookup = db.players.find({
                                "player_index.season": year_to_evaluate,
                                "player_index.name_stub": stub_name,
                                "player_index.team": team_code
                            })

                        metric_value = 0
                        prev_poss_count = 0

                        # calculate metric value (done in this way because to combine entries for players who were traded)
                        for prev_player_season in prev_season_player_lookup:
                            if "possessions" in prev_player_season:
                                try:
                                    prev_poss_count += prev_player_season["possessions"]
                                    #print("\t{} - {}: {} Poss {}".format(player["player"], metric, player[metric], player["possessions"]))
                                    metric_value += float(prev_player_season[metric]) * prev_player_season["possessions"]
                                except:
                                    print("error trying to convert to float ", metric, prev_player_season)
                            else:
                                metric_value = metrics_averages[metric]
                        if prev_poss_count > 750:
                            metric_value = metric_value / prev_poss_count
                        else:
                            metric_value = metrics_averages[metric]

                        try:
                            total_possessions += player["possessions"]
                            weighted_average += player["possessions"] * metric_value
                        except:
                            print(player)
                    else:
                        player_name = player["player"] if player.get("player") else "unknown_player_name"
                        team_code = player["team_id"] if player.get("team_id") else "unknown_team"
                        player_key = "{}_{}_{}".format(player_name, year, team_code)
                        players_without_possessions.add(player_key)
                    if total_possessions == 0:
                        print(players)
                        continue
                if total_possessions > 0:
                    weighted_average = weighted_average / total_possessions
                    print("{}_{} has a weighted average of {} {}\n".format(team_code, year_to_evaluate, weighted_average, metric))

                    db.teams.update_one({
                        "team_index.season": year,
                        "team_index.team_id": team_code
                    },
                    {
                        "$set": {
                            "Y-{}_{}".format(year_step, metric): weighted_average
                        }
                    })
                total_possessions = 0
                weighted_average = 0
            print("time took {} seconds for one year step".format(time.perf_counter()-timestamp))
    pprint(players_without_possessions)


def player_exists(stub_name,  season):
    player = db.players.find_one({
            "player_index.name_stub": stub_name,
            "player_index.season": season})
    return player is not None

def player_minus_jr_exists(stub_name, season):
    if stub_name is None:
        return False
    if stub_name[-2:] == "jr":
        stub_name = stub_name[:len(stub_name)-2]
    else:
        return False

    player = db.players.find_one({
            "player_index.name_stub": stub_name,
            "player_index.season": season})
    return player is not None

def player_jr_exists(stub_name, season):
    if stub_name is not None:
        stub_name += "jr"
    else: 
        return False

    player = db.players.find_one({
        "player_index.name_stub": stub_name,
        "player_index.season": season})

    return player is not None

def calculate_r_squared_values(year_start, year_end):
    metric_names = ["per", "bpm", "rapm", "apm", "rpm", "ws_per_48", "ts_pct", "wins"]
    metrics = []
    for year_step in range(4):
        for metric in metric_names:
            metrics.append("Y-{}_{}".format(year_step, metric))
    wins_metric = "wins"
    teams = list(db.teams.find({"team_index.season": {"$gte": int(year_start), "$lte": int(year_end)}}))
    result = {}

    for metric in metrics:
        X = []
        Y = []

        for team in teams:
            try:
                X.append(team[metric])
                Y.append(team[wins_metric])
            except KeyError as e:
                #print("key error for metric probably couldn't find rpm")
                pass
        Xnp = numpy.array(X)
        Ynp = numpy.array(Y)
        slope, intercept, r_value, p_value, std_err = stats.linregress(Xnp,Ynp)
        r_squared = r_value**2
        result[metric] = r_squared

    return result

def add_metric_for_years_prior(year, metric):
    teams = db.teams.find({"season": year})

    for team in teams:
        team_code = team["team_code"]
        for year_step in range(4):
            key_name = "Y-{}_{}".format(year_step, metric)
            print(key_name)
            year_to_find = year - year_step
            if year_to_find < 1997:
                print("no data prior to 1997")
                continue

            prev_team = db.teams.find_one({
                "team_index.team_id": team_code,
                "team_index.season": year_to_find
            })

            if prev_team is None:
                print("{}_{} not found, seeing if team was renamed".format(team_code, year_to_find))
                print(common_utils.team_renames)
                if team_code in common_utils.team_renames:
                    renamed_team_code = common_utils.team_renames[team_code]
                    prev_team = db.teams.find_one({
                        "team_index.team_id": renamed_team_code,
                        "team_index.season": year_to_find
                    })
                    if prev_team:
                        print("Renamed {}_{} found!".format(renamed_team_code, year_to_find))

                    else:
                        print("can't find renamed {}_{} either".format(renamed_team_code, year_to_find))
                        continue

                else:
                    raise RuntimeError("no rename, not sure what the deal is with {}_{}".format(team_code, year_to_find))

            #print(prev_team)
            db.teams.update_one({
                "team_index.season": year,
                "team_index.team_id": team_code
            },
            {
                "$set": {
                    key_name: prev_team[metric]
                }
            })


def convert_r_squared_result_to_csv(r_squared_dict, filename):
    for year_step in range(4):
        metrics = {}
        for key in r_squared_dict:
            print(key)
            metric_name = key[4:]
            if metric_name not in metrics:
                metrics[metric_name] = []
            metrics[metric_name].append(r_squared_dict[key])

    with open(filename, "w") as csvfile:
        writer = csv.writer(csvfile)


        for metric in metrics:
            row = [metric] + metrics[metric]
            print(row)
            writer.writerow(row)

        
def print_outputs_for_paper():
    print("2017 only")
    convert_r_squared_result_to_csv(calculate_r_squared_values(2017, 2017), "2017_only.csv")
    print("2017 and 2018 seasons")
    convert_r_squared_result_to_csv(calculate_r_squared_values(2017, 2019), "2016-18_only.csv")
    print("1997 - 2018 seasons")
    convert_r_squared_result_to_csv(calculate_r_squared_values(1997, 2019), "1997-2018.csv")


if __name__ == "__main__":
    #work_used_to_generate_map_codes()
    # for year in range(1997, 2018+1):
    #     add_metric_for_years_prior(year, "wins")
        #calculate_weighted_average(year)
    print_outputs_for_paper()
    #pprint(players_without_possessions)


    # result = calculate_r_squared_values(2017, 2017)
    # convert_r_squared_result_to_csv(result, "2017_only.csv")