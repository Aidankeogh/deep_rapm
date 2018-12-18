import copy
from pprint import pprint
from fields import *
import mongo_config
import time

import pymongo
from pymongo import MongoClient
client = MongoClient(mongo_config.host, mongo_config.port)
db = client.nba

import common_utils

def get_message(event):
    if event[event_field["HOMEDESCRIPTION"]]:
        return "[H] {}".format(event[event_field["HOMEDESCRIPTION"]])
    elif event[event_field["NEUTRALDESCRIPTION"]]:
        return "[N] {}".format(event[event_field["NEUTRALDESCRIPTION"]])
    elif event[event_field["VISITORDESCRIPTION"]]:
        return "[V] {}".format(event[event_field["VISITORDESCRIPTION"]])

def print_event(event):
    print("[Q{}] [{:>5}] [E{:03d}] [T:{:02d}] [MT:{:03d}] {}".format(
      event[event_field["PERIOD"]], # quarter
      event[event_field["PCTIMESTRING"]], # timestamp for quarter
      event[event_field["EVENTNUM"]], # event number
      event[event_field["EVENTMSGTYPE"]], # event type
      event[event_field["EVENTMSGACTIONTYPE"]], # event sub type
      get_message(event)))

def get_team_info(year_string,game_id,season_type):
    document = db.games[season_type][year_string].find_one({"game_index": game_id})
    team_info = {
        "away_team": document["away"],
        "home_team": document["home"]
    }
    return team_info

player_id_corrections = {
    471: ("Lionel Simmons", 1489),
    775: ("Melvin Booker", 511)
}

def get_and_parse_event_list_for_lineups(event_list, team_info, year):
    """
    Take rowSet of events from API call and return a list of equal size with the 5 man lineup on the floor for each team for each event
    Example output format:
    [
        {
            "Lakers": ["Lonzo Ball", "Brandon Ingram", "Corey Brewer", "Julius Randle", "Brook Lopez"],
            "Warriors": ["Stephen Curry", "Kevin Durant", "Klay Thompson", "Draymond Green", "Andre Iguodala"],
            "event_num": 2
        }, ...
    ]
    """
    player_ids = {}

    year_as_int = int(year)

    home_team = team_info["home_team"]
    away_team = team_info["away_team"]

    lineups = {
        team_info["home_team"]: set(),
        team_info["away_team"]: set()
    }

    event_with_lineups_list = []
    current_quarter = -1

    for index, event in enumerate(event_list):
        #print(event_with_lineups_list)
        # reset lineups when quarter changes
        if current_quarter != event[event_field["PERIOD"]]:
            current_quarter = event[event_field["PERIOD"]]
            lineups[team_info["home_team"]] = set()
            lineups[team_info["away_team"]] = set()
            quarter_start_index = index

        # this piece of code is for a single empty event toward the end of this game: 
        # http://stats.nba.com/game/0020000883/playbyplay/ that causes problems otherwise
        if event[event_field["EVENTMSGTYPE"]] == event_type["SUB"] and get_message(event) is None:
            pass
        # substitution event
        # player 1 is being substituted out
        # player 2 is coming in
        elif event[event_field["EVENTMSGTYPE"]] == event_type["SUB"]:
            #print_event(event)
            team = common_utils.convert_nba_espn_team_codes_to_bball_ref(event[event_field["PLAYER1_TEAM_ABBREVIATION"]], year_as_int)
            try:
                player_to_sub_out = common_utils.nba_com_player_name_to_bball_ref_player_name(event[event_field["PLAYER1_NAME"]], team, year_as_int)
            except RuntimeError as e:
                print_event(event)
            try:
                player_to_sub_in  = common_utils.nba_com_player_name_to_bball_ref_player_name(event[event_field["PLAYER2_NAME"]], team, year_as_int)
            except RuntimeError as e:
                print_event(event)

            if (player_to_sub_out, team, year_as_int) not in player_ids:
                player_ids[(player_to_sub_out, team, year_as_int)] = event[event_field["PLAYER1_ID"]]
            if ( player_to_sub_in, team, year_as_int) not in player_ids:
                player_ids[( player_to_sub_in, team, year_as_int)] = event[event_field["PLAYER2_ID"]]

            if player_to_sub_out not in lineups[team]:
                # backfill
                for e in event_with_lineups_list[quarter_start_index:]:
                    e[team].add(player_to_sub_out)

            lineups[team].discard(player_to_sub_out)
            lineups[team].add(player_to_sub_in)

        event_with_lineups_list.append({
            team_info["home_team"] : lineups[team_info["home_team"]].copy(),
            team_info["away_team"] : lineups[team_info["away_team"]].copy(),
            "event_num": event[event_field["EVENTNUM"]],
        })

        # player1, player2, player3
        if event[event_field["EVENTMSGTYPE"]] != event_type["SUB"]:
            f = [event_field['PLAYER1_NAME'], event_field['PLAYER2_NAME'], event_field['PLAYER3_NAME']]
            for p_index in f:
                # team code is always an offset of 4 from player name
                if event[p_index] and event[p_index + 4]:

                    team_code = common_utils.convert_nba_espn_team_codes_to_bball_ref(event[p_index + 4], year_as_int)
                    try:
                        player_name = common_utils.nba_com_player_name_to_bball_ref_player_name(event[p_index], team_code, year_as_int)
                    except RuntimeError as e:
                        print(e)
                    if (player_name, team_code, year_as_int) not in player_ids:
                        # player id is offset -1 from player name
                        player_ids[(player_name, team_code, year_as_int)] = event[p_index - 1]

                    if len(lineups[team_code]) < 5 and player_name not in lineups[team_code]:
                        lineups[team_code].add(player_name)

                        # backfill
                        for e in event_with_lineups_list[quarter_start_index:]:
                            e[team_code].add(player_name)

    for (t_player_name, t_team_code, t_year) in player_ids:
        stub_name = common_utils.player_to_stub_name(t_player_name)
        player = db.players.update_one({
            "player_index.name_stub": stub_name,
            "player_index.team": t_team_code,
            "player_index.season": t_year
        },
        {
            "$set": {
                "nba_com_id": player_ids[(t_player_name, t_team_code, t_year)]
            }
        })


    return event_with_lineups_list

def get_lineups_and_possession_info(game_id, event_list, team_info, year):
    """takes in a gameid from stats.nba.com and parses play by play to get data in the following format:
    [ {
        "home_lineup": [playerid1, ..., playerid5],
        "away_lineup": [playerid6, ..., playerid10],
        "score_margin_update": 2, #(0-4 range of points that can be scored on a possession, not accounting for weirdness that can happen with technicals, flagrants)
        "home_team_is_on_offense": True, # or false,
        "possession_metadata": 3 #any data we want to keep about what the event was, at least event number for reference
       }, {...}, ...
    ]
    """
    event_with_lineups_list = get_and_parse_event_list_for_lineups(event_list, team_info, year)

    home_team = team_info["home_team"]
    away_team = team_info["away_team"]

    initial_free_throw_keys = ["1of2", "1of3", "tech"]
    subsequent_free_throws_keys = ["2of2", "2of3", "3of3"]

    initial_free_throws = [free_throw_event_type[key] for key in initial_free_throw_keys]
    subsequent_free_throws = [free_throw_event_type[key] for key in subsequent_free_throws_keys]

    possession_event_list = []


    for index, event in enumerate(event_list):
        # print_event(event)
        team = event[event_field["PLAYER1_TEAM_ABBREVIATION"]]
        # made shot
        possession_event = {
            "home_lineup": list(event_with_lineups_list[index][home_team]),
            "away_lineup": list(event_with_lineups_list[index][away_team]),
            "scoring_margin_update": 0,
            "home_team_is_on_offense": team == home_team,
            "possession_metadata": {
                "event_num": event[event_field["EVENTNUM"]],
                "message": get_message(event),
                "gid": game_id,
                "event_type": event[event_field["EVENTMSGTYPE"]]
            }
        }

        if event[event_field["EVENTMSGTYPE"]] == event_type["FGM"]:
            if get_message(event):
                possession_event['scoring_margin_update'] = 3 if "3PT" in get_message(event) else 2
            possession_event_list.append(possession_event)


        # free throws
        # logic here is to add a scoring possession on the first free throw, then update that possession
        # for any further made free throws. Substitutions almost always come after the first free throw so this takes
        # care of that. In and one situation, update previous bucket with extra point if free throw made
        elif event[event_field["EVENTMSGTYPE"]] == event_type["FTA"]:

            free_throw_event_code = event[event_field["EVENTMSGACTIONTYPE"]]
            if free_throw_event_code in initial_free_throws:
                possession_event['scoring_margin_update'] = 1 if "MISS" not in get_message(event) else 0
                possession_event_list.append(possession_event)
            elif free_throw_event_code in subsequent_free_throws:
                back_iter = len(possession_event_list) - 1
                if "MISS" not in get_message(event):
                    # scan back to find previous free throw event
                    while back_iter > 0 and possession_event_list[back_iter]["possession_metadata"]["event_type"] != event_type["FTA"]:
                        back_iter += -1
                    if possession_event_list[back_iter]["possession_metadata"]["event_type"] == event_type["FTA"]:
                        possession_event_list[back_iter]["scoring_margin_update"] += 1

            elif free_throw_event_code == free_throw_event_type["1of1"]:
                back_iter = len(possession_event_list) - 1

                if "MISS" not in get_message(event):
                    # scan back to find previous made bucket
                    while back_iter > 0 and possession_event_list[back_iter]["possession_metadata"]["event_type"] != event_type["FGM"]:
                        back_iter += -1
                    if possession_event_list[back_iter]["possession_metadata"]["event_type"] == event_type["FGM"]:
                        possession_event_list[back_iter]["scoring_margin_update"] += 1

        # rebounds
        elif event[event_field["EVENTMSGTYPE"]] == event_type["REB"]:

            team_of_rebounder = event[event_field["PLAYER1_TEAM_ABBREVIATION"]]
            team_of_player_who_missed = event_list[index-1][event_field["PLAYER1_TEAM_ABBREVIATION"]]

            if team_of_rebounder != team_of_player_who_missed:
                possession_event['scoring_margin_update'] = 0
                possession_event['home_team_is_on_offense'] = home_team == team_of_player_who_missed
                possession_event_list.append(possession_event)

        # steals / turnovers
        elif event[event_field["EVENTMSGTYPE"]] == event_type["STL"]:
            if get_message(event):
                if "steal" in get_message(event).lower():
                    stealers_team = event[event_field["PLAYER1_TEAM_ABBREVIATION"]]
                    home_team_is_on_offense = stealers_team == home_team
                elif "turnover" in get_message(event).lower():
                    to_team = event[event_field["PLAYER1_TEAM_ABBREVIATION"]]
                    home_team_is_on_offense = to_team == home_team
                else:
                    raise RuntimeError("Confused by this event: {}".format(get_message(event)))

                possession_event['home_team_is_on_offense'] = home_team_is_on_offense
                possession_event_list.append(possession_event)

    return possession_event_list

def save_lineup_data_for_season(year, season_type="regular_season"):
    year_string = common_utils.construct_year_string(year)

    print('creating index')
    db.possessions[season_type][year_string].create_index([
        ('possession_metadata.event_num', pymongo.ASCENDING),
        ('possession_metadata.gid', pymongo.ASCENDING)
    ], unique=True)
    print('index created (or already existed)')

    games = db.games[season_type][year_string].find()
    num_games = games.count()
    prev_time = time.perf_counter()
    for index, game in enumerate(games):
        #if game["game_index"]:
        # if game["game_index"] == "0049900030":
        print("processing {}...".format(game["game_index"]))
        team_info = get_team_info(year_string, game["game_index"], "playoffs")
        data = get_lineups_and_possession_info(game["game_index"], game["pbp"], team_info, year)
        for possession in data:
            db.possessions[season_type][year_string].update({
                'possession_metadata.gid': possession['possession_metadata']['gid'],
                'possession_metadata.event_num': possession['possession_metadata']['event_num']
            }, possession, True)
        if index % 10 == 0:
            curr_time = time.perf_counter()
            print("time elapsed: {}".format(curr_time - prev_time))
            prev_time = curr_time
            print("Processed {} / {} games for {} season".format(index+1, num_games, year_string))


if __name__ == "__main__":
    for year in range(2000,2018):
        save_lineup_data_for_season(year, "playoffs")
