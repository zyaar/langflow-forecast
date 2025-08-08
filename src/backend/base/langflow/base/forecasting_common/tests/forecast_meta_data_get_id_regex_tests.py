import re
import nanoid
from langflow.base.forecasting_common.models.forecast_meta_data import ForecastMetaDataSeriesIdGenerator

def main():
    #re_full_value = r"^\s*([\w-]+)\.(?!.*\.)"
    
    ##re_rel_id = r"\.(?!.*\.)([\w-]+)(:|\[|$)"
    ##re_rel_id2 = r"^([\w-]+)(:|\[|$)"
    ##re_single_value = r":(\d+)[\[|$]"

    re_full_value = ForecastMetaDataSeriesIdGenerator.full_match_regex
    re_rel_value = ForecastMetaDataSeriesIdGenerator.rel_match_regex
    re_shift_value = ForecastMetaDataSeriesIdGenerator.shift_match_regex
    re_single_value = ForecastMetaDataSeriesIdGenerator.single_match_regex

    #re_shift_value = r"\[(-?\d+)\]\s*$"
    #re_single_value = r"[^:+]:(-?\d+)" + re_shift_value

    full_value_tests = [
        ["", False],
        #["Component_ik5cL", True, "Component_ik5cL"],
        ["Component_ik5cL..", False],
        ["Compone.nt_ik5cL.", False],
        [" Component_ik5cL.", True, "Component_ik5cL"],
        ["  Component_ik5cL.", True, "Component_ik5cL"],
        #["Component_ik5cL", False],
    ]

    rel_value_tests = [
        ["", False],
        ["Row_EXwbM", True, "Row_EXwbM"],
    ]

    shift_value_tests = [["", False],
                         ["[1]",  True, 1],
                         ["[10]",  True, 10],
                         ["[100]",  True, 100],
                         ["[-1]",  True, -1],
                         ["[-10]",  True, -10],
                         ["[-100]",  True, -100],
                         ["[--1]",  False],
                         ["[--10]",  False],
                         ["[--100]",  False],
                         ["[-1-]",  False],
                         ["[-10-]",  False],
                         ["[-100-]",  False],
                         ["[1-]",  False],
                         ["[10-]",  False],
                         ["[100-]",  False],
                         ["[1] ",  True, 1],
                         ["[-10] ",  True, -10],
                         ["[-100]  ",  True, -100],
                         ["[[1]]",  False],
                         ["[[-10]]",  False, -10],
                         ["[[-100]]",  False, -100],]
    
    single_value_tests = [
        ["", False],
        [":3", True, 3],
        [":33", True, 33],
        [":333", True, 333],
        [":-3", True, -3],
        [":-33", True, -33],
        [":-333", True, -333],
        ["::3", False],
        ["::-33", False],
        ["::-333", False],
    ]

    for test_case_rel in rel_value_tests:
        for test_case_full in full_value_tests:
            for test_case_single in single_value_tests:
                for test_case_shift in shift_value_tests:
                    test_case = test_case_full[0] + test_case_rel[0] + test_case_single[0] + test_case_shift[0]
                    should_be_match = all([test_case_full[1], test_case_rel[1], test_case_single[1], test_case_shift[1]])

                    val_if_true_full = None
                    val_if_true_rel = None
                    val_if_true_single = None
                    val_if_true_shift = None

                    if test_case_full[1]:
                        val_if_true_full = test_case_full[2]

                    if test_case_rel[1]:
                        val_if_true_rel = test_case_rel[2]

                    if test_case_single[1]:
                        val_if_true_single = test_case_single[2]

                    if test_case_shift[1]:
                        val_if_true_shift = test_case_shift[2]
                    
                    test_one_case("full", test_case, re_full_value, should_be_match, test_case_full[1], val_if_true_full)
                    test_one_case("single", test_case, re_single_value, should_be_match, test_case_single[1], val_if_true_single)
                    test_one_case("shift", test_case, re_shift_value, should_be_match, test_case_shift[1], val_if_true_shift)


                    # rel test, first try to remove the full, then run the rel
                    is_match = re.search(re_full_value, test_case)

                    if(is_match):
                        test_case = test_case.removeprefix(is_match[0])

                    test_one_case("rel", test_case, re_rel_value, should_be_match, test_case_rel[1], val_if_true_rel)


def test_one_case(test_type, test_string, test_regex, should_match, should_match_indiv, val_if_true = None):
    is_match = re.search(test_regex, test_string)

    # check if there is no match
    if(is_match is None):
        # if there should have been a match raise an error
        if should_match:
            print(f"match missed ({test_type}): '{test_string}'")
            return
        
    # if there was a match
    else:
        # check if it should match under the least restrictive circumstances
        if should_match_indiv:
            # check that it got the correct value
            if(is_match[1]) != str(val_if_true):
                print(f"did not get correct value ({test_type}): '{is_match[1]}' != '{val_if_true}'")
                return
            
        # it should not have matched
        else:
            print(f"false match ({test_type}): '{test_string}'")
            return 


if __name__ == "__main__":
    main()
