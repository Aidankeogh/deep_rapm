import pickle

from pprint import pprint

import calculate_rapm
import evaluate_metrics
import parse_pbp
import common_utils

if __name__ == "__main__":
    for year in range(2017,2019):
        year_string = common_utils.construct_year_string(year)
        # #parse_pbp.save_lineup_data_for_season(str(year))
        # calculate_rapm.store_games_data(year)
        # calculate_rapm.store_player_and_possession_data_for_matrix(year)
        # X, Y = calculate_rapm.build_matrix(year)
        # X_loaded = ""
        # Y_loaded = ""
        # with open("matrices/{}-X-indicator.pickle".format(year_string), "rb") as picklefile:
        #     X_loaded = pickle.load(picklefile)
        # with open("matrices/{}-Y.pickle".format(year_string), "rb") as picklefile:
        #     Y_loaded = pickle.load(picklefile)
            
        # calculate_rapm.calculate_rapm(year, X_loaded, Y_loaded, "apm")
        # calculate_rapm.calculate_rapm(year, X_loaded, Y_loaded, "rapm")
        # calculate_rapm.calculate_rapm(year, X_loaded, Y_loaded, "rapm_enet_cv")
        # calculate_rapm.calculate_rapm(year, X_loaded.toarray(), Y_loaded, "rapm_bayes_ridge")
        # evaluate_metrics.calculate_weighted_average(year)
        pprint(evaluate_metrics.calculate_r_squared_values(year))
