import pymongo

from pymongo import MongoClient

import mongo_config
client = MongoClient(mongo_config.host, mongo_config.port)
db = client.nba


# espn and nba.com team codes mapped to bball ref codes
team_codes_to_bball_ref_codes = {
    "NY": "NYK",
    "PHX": "PHO",
    "NO": "NOP",
    "GS": "GSW",
    "WSH": "WAS",
    "BKN": "BRK",
    "SA": "SAS",
    "UTAH": "UTA"
}

nba_com_stubs_to_bball_ref_stubs = {
    "stevensmith": "stevesmith", 
    "slavamedvedenko": "stanislavmedvedenko",
    "flipmurray": "ronaldmurray",
    "normanrichardson": "normrichardson",
    "nene": "nenehilario",
    "michaelsweetney": "mikesweetney",
    "djmbenga": "didierilungambenga",
    "clarweatherspoon": "clarenceweatherspoon",
    "richardmanning": "richmanning",
    "ikeaustin": "isaacaustin",
    "danschayes": "dannyschayes",
    "horatiollamas": "horaciollamasgrey",
    "horaciollamas": "horaciollamasgrey",
    "horatiollamasgrey": "horaciollamasgrey",
    "jeffsheppard": "jeffreysheppard",
    "charlesrjones": "charlesjones",
    "markbaker": "lamarkbaker",
    "ikefontaine": "isaacfontaine",
    "perryjonesiii": "perryjones",
    "gigidatome": "luigidatome",
    "poohjeter": "eugenejeter",
    "larrydrewii": "larrydrew",
    "jamesennisiii": "jamesennis",
    "juanchohernangomez": "juanhernangomez",
    "andrewwhiteiii": "andrewwhite",
    "walterlemonjr": "waltlemonjr",
    "glennrobinsoniii": "glennrobinson",
    "garypaytonii":"garypayton",
    "johnnyobryantiii": "johnnyobryant",
    "taureanprince": "taureanwallerprince",
    "wadebaldwiniv": "wadebaldwin",
    "efthimiosrentzias": "efthimirentzias",
    "ibrahimkutluay": "ibokutluay",
    "haha": "haseungjin",
    "sunsun": "sunyue",
    "jameswebbiii": "jameswebb",
    "wesiwundu": "wesleyiwundu",
    "vincenthunter": "vincehunter",
    "frankmasoniii": "frankmason", 
    "maximiliankleber": "maxikleber"

}

# maps current code to a previous years code
team_renames = {
    "NOP": "NOH",
    "NOH": "NOK",
    "NOK": "NOH",
    "CHO": "CHA",
    "CHA": "CHH",
    "MEM": "VAN",
    "OKC": "SEA",
    "WAS": "WSB", 
    "BRK": "NJN"
}

def construct_year_string(year):
    return str(int(year)-1) + '-' + str(year)[2:]

def player_to_stub_name(player_name):
    if player_name is None:
        return
    characters_to_wipe = " .'-,*"
    player_name_stub = player_name
    for c in characters_to_wipe:
        player_name_stub = player_name_stub.replace(c, "")
    return player_name_stub.lower()

def player_exists(stub_name, team, season):
    player = db.players.find_one({
            "player_index.name_stub": stub_name,
            "player_index.team": team,
            "player_index.season": season})
    return player is not None

def player_minus_jr_exists(stub_name, team, season):
    season_as_int = int(season)
    if stub_name is None:
        # print("stub name is None")
        return False
    if stub_name[-2:] == "jr":
        # print("stub name does have jr at end")
        stub_name = stub_name[:len(stub_name)-2]
    else:
        # print("stub name does not have jr at end")
        return False

    # print("{}_{}_{}".format(stub_name, team, season))
    player = db.players.find_one({
            "player_index.name_stub": stub_name,
            "player_index.team": team,
            "player_index.season": season_as_int})

    return player is not None

def memoize(f):
    memo = {}
    def helper(x, y, z):
        if x not in memo:            
            memo[x] = f(x, y, z)
        return memo[x]
    return helper

@memoize
def nba_com_player_name_to_bball_ref_player_name(player_name, team_code, year):
    year_as_int = int(year)
    stub_name = player_to_stub_name(player_name)

    # hard coding this Hornets nonsense
    # ESPN's code on RPM pages for the Hornets is CHA
    # Bball-ref's code for the Bobcats is CHA, changed to CHO when Bobcats became the hornets after the 2014 season
    # So CHA is not supposed to be converted to CHO, unless it's after 2014
    if team_code == "CHA" and year_as_int > 2014:
        team_code = "CHO"

    if team_code == "WAS" and year_as_int < 1998:
        team_code = "WSB"


    # unset stevensmith key in year 2007 cause there is a real stevensmith then
    if year_as_int == 2007 and "stevensmith" in nba_com_stubs_to_bball_ref_stubs:
        del nba_com_stubs_to_bball_ref_stubs["stevensmith"]
    else:
        nba_com_stubs_to_bball_ref_stubs["stevensmith"] = "stevesmith"

    if player_exists(stub_name, team_code, year_as_int):
        return player_name

    print("[{}] {}'s name doesn't match bball-ref name. Attempting to resolve...".format(team_code, player_name))
    print("\tstubname is {}".format(stub_name))

    if player_minus_jr_exists(stub_name, team_code, year):
        base_name = player_name[:len(player_name)-4]
        print("\tConverted {} to {}".format(player_name, base_name))
        return base_name

    elif stub_name in nba_com_stubs_to_bball_ref_stubs:
        player = db.players.find_one({
            "player_index.name_stub": nba_com_stubs_to_bball_ref_stubs[stub_name],
            "player_index.team": team_code,
            "player_index.season": year_as_int
        })
        if player is None:
            raise RuntimeError("Something went wrong. Should have, but did not find {}_{}_{}".format(nba_com_stubs_to_bball_ref_stubs[stub_name], team_code, year_as_int))
        print("\tFound harded coded version from {} to {}".format(player_name, player["player"]))
        return player["player"]

    else:
        print("{}_{}_{} not found, assuming it is a playoffs only player who didn't play in regular season and inserting".format(stub_name, team_code, year_as_int))
        db.players.insert_one({
            "player_index": {
                "name_stub": stub_name,
                "team": team_code,
                "season": year_as_int
            }
        })

        #raise RuntimeError("[{}] {}'s name was unresolved".format(team_code, player_name))

def convert_nba_espn_team_codes_to_bball_ref(team_code, year):
    if team_code == "CHA" and int(year) > 2014:
        return "CHO"
    if team_code in team_codes_to_bball_ref_codes:
        return team_codes_to_bball_ref_codes[team_code]
    else:
        return team_code