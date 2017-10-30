#!/usr/bin/env python
from __future__ import print_function

from criteo_starter_kit.criteo_dataset import CriteoDataset
from criteo_starter_kit.criteo_prediction import CriteoPrediction

import numpy as np

DEBUG = False
GOLD_LABEL_PATH = "data/cntk_train_small.txt"
PREDICTIONS_PATH = "data/predictions.txt"

def grade_predictions(PREDICTIONS_PATH, GOLD_LABEL_PATH):
    gold_data = CriteoDataset(GOLD_LABEL_PATH)
    predictions = CriteoPrediction(PREDICTIONS_PATH)

    # Instantiate variables
    pos_label = 0.999
    neg_label = 0.001

    max_instances = predictions.max_instances

    num_positive_instances = 0
    num_negative_instances = 0

    #Random
    rand_numerator = np.zeros(max_instances, dtype = np.float)
    rand_denominator = np.zeros(max_instances, dtype = np.float)

    #Logger
    log_numerator = np.zeros(max_instances, dtype = np.float)
    log_denominator = np.zeros(max_instances, dtype = np.float)

    #NewPolicy
    prediction_numerator = np.zeros(max_instances, dtype = np.float)
    prediction_denominator = np.zeros(max_instances, dtype = np.float)

    #NewPolicy - Stochastic
    prediction_stochastic_numerator = np.zeros(max_instances, dtype = np.float)
    prediction_stochastic_denominator = np.zeros(max_instances, dtype = np.float)

    impression_counter = 0
    for _idx, _impression in enumerate(gold_data):
        # TODO: Add Validation
        prediction = next(predictions)
        scores = prediction["scores"]

        label = _impression["cost"]
        propensity = _impression["propensity"]
        num_canidadates = len(_impression["candidates"])

        rectified_label = 0

        if label == pos_label:
            rectified_label = 1
            num_positive_instances += 1
        elif label == neg_label:
            num_negative_instances += 1
        else:
            # TODO: raise Error
            pass

        rand_weight = 1.0 / (num_canidadates * propensity)
        rand_numerator[_idx] = rectified_label * rand_weight
        rand_denominator[_idx] = rand_weight


        if label == pos_label:
            log_weight = 1.0
        elif label == neg_label:
            log_weight = 10.0
        else:
            #TODO: raise Error
            pass

        log_numerator[_idx] = rectified_label * log_weight
        log_denominator[_idx] = log_weight

        #For deterministic policy
        best_score = np.min(scores)
        best_classes = np.argwhere(scores == best_score).flatten()

        #For stochastic policy
        score_logged_action = None
        score_normalizer = 0.0
        score_offset = -best_score

        scores_with_offset = -scores - score_offset
        prob_scores = np.exp(scores_with_offset)

        score_normalizer = np.sum(prob_scores)

        score_logged_action = prob_scores[0]

        if 0 in best_classes:
            prediction_weight = 1.0 / (len(best_classes) * propensity)
            prediction_numerator[_idx] = rectified_label * prediction_weight
            prediction_denominator[_idx] = prediction_weight

        prediction_stochastic_weight = 1.0 * score_logged_action / (score_normalizer * propensity)
        prediction_stochastic_numerator[_idx] = rectified_label * prediction_stochastic_weight
        prediction_stochastic_denominator[_idx] = prediction_stochastic_weight

        impression_counter += 1 #Adding this as _idx is not available out of this scope
        if _idx % 100 == 0 and DEBUG:
            print('.', end='')

    gold_data.close()
    predictions.close()

    modified_denominator = num_positive_instances + 10*num_negative_instances
    scaleFactor = np.sqrt(max_instances) / modified_denominator

    if DEBUG:
        print('')
        print("Num[Pos/Neg]Test Instances:", num_positive_instances, num_negative_instances)
        print("MaxID; curId", max_instances, impression_counter)
        print("Approach & IPS(*10^4) & StdErr(IPS)*10^4 & SN-IPS(*10^4) & StdErr(SN-IPS)*10^4 & AvgImpWt & StdErr(AvgImpWt) \\")


    def compute_result(approach, numerator, denominator):
        IPS = numerator.sum(dtype = np.longdouble) / modified_denominator
        IPS_std = 2.58 * numerator.std(dtype = np.longdouble) * scaleFactor        #99% CI
        ImpWt = denominator.sum(dtype = np.longdouble) / modified_denominator
        ImpWt_std = 2.58 * denominator.std(dtype = np.longdouble) * scaleFactor    #99% CI
        SNIPS = IPS / ImpWt

        normalizer = ImpWt * modified_denominator

        #See Art Owen, Monte Carlo, Chapter 9, Section 9.2, Page 9
        #Delta Method to compute an approximate CI for SN-IPS
        Var = np.sum(np.square(numerator) +\
                        np.square(denominator) * SNIPS * SNIPS -\
                        2 * SNIPS * np.multiply(numerator, denominator), dtype = np.longdouble) / (normalizer * normalizer)

        SNIPS_std = 2.58 * np.sqrt(Var) / np.sqrt(max_instances)                 #99% CI

        _response = {}
        _response["ips"] = IPS*1e4
        _response["ips_std"] = IPS_std*1e4
        _response["snips"] = SNIPS*1e4
        _response["snips_std"] = SNIPS_std*1e4
        _response["impwt"] = ImpWt
        _response["impwt_std"] = ImpWt_std
        _response["max_instances"] = max_instances

        return _response

    return compute_result('NewPolicy-Stochastic', prediction_stochastic_numerator, prediction_stochastic_denominator)

if __name__ == "__main__":
    DEBUG = True
    print(grade_predictions(PREDICTIONS_PATH, GOLD_LABEL_PATH))
