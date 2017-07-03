import calendar
import datetime

import pytz
from dateutil.relativedelta import *


SIGN_NEGATIVE = "negative"
SIGN_POSITIVE = "positive"

def to_server_tz(dt, biz):
    src_tz_info = timezone('US/Pacific')
    biz_tz_info = timezone(biz.biz_timezone)
    return biz_tz_info.localize(dt).astimezone(src_tz_info).replace(tzinfo=None)

def get_tz_time_diff(biz):
    """
    Determines the time difference from Server (America/Los_Angeles).
    The result is in Miliseconds.
    Args:
        @biz (instance): Biz model instance.
    Return:
        @difference (int): difference in milisecods
        @sign (str): sign of time difference, two possible values (SIGN_NEGATIVE, SIGN_POSITIVE)
    """
    src_time = datetime.datetime.now().replace(tzinfo=None)
    biz_time = biz.get_localised_datetime_for(src_time).replace(tzinfo=None)
    rdelta = relativedelta(src_time, biz_time)

    total_microseconds = 0
    if rdelta.hours: total_microseconds += (abs(rdelta.hours) * 60 * 60 * 1000)
    if rdelta.minutes: total_microseconds += (abs(rdelta.minutes) * 60 * 1000)
    if rdelta.seconds: total_microseconds += (abs(rdelta.seconds) * 1000)
    if rdelta.microseconds: total_microseconds += abs(rdelta.microseconds)

    if total_microseconds == 0:
        return total_microseconds, None
    else:
        non_none_entry = rdelta.hours if rdelta.hours else rdelta.minutes if rdelta.minutes else rdelta.seconds
        sign = SIGN_NEGATIVE if non_none_entry < 0 else SIGN_POSITIVE
        return total_microseconds, sign

def get_vlaues_list_from_cron_expr(expr):
    """
    Convert cron expression string to list of possible integer values.

    Args:
        @expr (str): Individual part of cron expression (Ex. minute part or hour part)
    Return:
        List of possible integer values derived from cron expression string.
    """
    expr = expr.strip()
    if expr == "*": return []

    if "," in expr:
        expr_list = expr.split(",")
    elif "-" in expr:
        count_start, count_end = sorted(expr.split("-"))
        return [i for i in xrange(int(count_start), int(count_end) + 1)]
    else:
        return [int(expr)]

    expr_list_to_return = list()
    for e in expr_list:
        e = e.strip()
        if "-" in e:
            count_start, count_end = e.split("-")
            expr_list_to_return = expr_list_to_return + [i for i in xrange(int(count_start), int(count_end) + 1)]
        else:
            expr_list_to_return.append(int(e))
    return expr_list_to_return

def get_cron_value(expr_list):
    if len(expr_list) == 0: return "*"

    expr_list = sorted(expr_list)

    cron = [str(expr_list[0])]
    s = expr_list[0]
    c = expr_list[0]
    n = len(expr_list)

    for i in xrange(1, n):
        if expr_list[i] == c + 1:
            c = expr_list[i]
            cron[len(cron) - 1] = "%s-%s" % (s, c)
        else:
            s = expr_list[i]
            c = expr_list[i]
            cron.append(str(c))

    return ",".join(cron)


def make_cron_expression_from_lists(dow, moy, dom, hour, _min):
    """
    Gets values of every part of cron as a list and convert it to whole
    Cron string expression.
    Args:
        @dow: list of day of week values.
        @moy: list of month of year values.
        @dom: list of day of month values.
        @hour: list of hour values.
        @_min: list of minute values.
    Return:
        Cron expression in string format.
    """
    return " ".join([
        get_cron_value(_min),
        get_cron_value(hour),
        get_cron_value(dom),
        get_cron_value(moy),
        get_cron_value(dow)
    ])

def left_shift_cron_expr_list(expr_list, _type, **kwargs):
    """
    Shift values to left by one, of weeks, month days and months.

    Args:
        @expr_list (list of int): List of values to be shift by left.
        @_type (str): Type of list, Possible Values are
                      `dow`(Day of week), `moy`(Month of Year), `dom`(Day of Month)
    Return:
        Shifted list

    NOTE:
        As all our merchants are INDIAN, so every ones time zone negative from 
        Server timezone, So always be left shifted.
    TODO:
        Make dynamic (left or right shift) according to the timezone sign.
    """
    n = 7 if _type == "dow" else 12 if _type == "moy" else 31
    total_list = [i for i in xrange(1, n + 1)]

    expr_list_to_return =[]
    for expr in expr_list:
        expr_list_to_return.append(total_list[total_list.index(expr) - 1])

    return list(set(expr_list_to_return))

def convert_cron_to_tz(cron_expr, biz):
    # Get the time difference in miliseconds between server tz and biz tz
    diff_in_ms, sign = get_tz_time_diff(biz)

    # Store Every part of Cron expression in variables
    cron_vlues = cron_expr.split(" ")
    src_cron_min, src_cron_hour, src_cron_dom, src_cron_moy, src_cron_dow = cron_vlues
    
    # We are not supporting Every or any Consecutive Minutes or hours
    if ("*" in src_cron_min or "-" in src_cron_min or "," in src_cron_min) \
        and ("*" in src_cron_hour or "-" in src_cron_hour):
        raise ValueError("Campaign can be atmost once in every 2 hours")

    # Convert Min and Hour Cron Expression to list of vlaues.
    src_cron_min_list = get_vlaues_list_from_cron_expr(src_cron_min)
    src_cron_hour_list = get_vlaues_list_from_cron_expr(src_cron_hour)

    # Minimum Difference between two consecutive Hour should be atlest 2
    if len(src_cron_hour_list) > 12 or (len(src_cron_hour_list) >= 2 \
        and any(src_cron_hour_list[i] - src_cron_hour_list[i-1] < 2 for i in xrange(1, len(src_cron_hour_list)))):
        raise ValueError("Difference beteween two consecutive hours should be at least 2 hours")

    # If Same Timezone retun same Cron Expression.
    if diff_in_ms == 0: return cron_expr

    """ 
    Boolean Indicator to see if Some cron time touch previous/next day after Converting to server timezone.
    It will be At most 20 hours difference from Los Angeles to any other timezone.
    So it'll only touch previous/next day, no option for touching 2 days prev/next.
    """
    touched_previous_day = False

    """
    Indicator if some cron expression not touching previous/next day
    Only changes the time in the same date.
    """
    in_same_day = False

    # Get list of values from cron expression of `Day of Week`, `Month Of Year` and `Day Of Month`
    src_cron_dom_list = get_vlaues_list_from_cron_expr(src_cron_dom)
    src_cron_moy_list = get_vlaues_list_from_cron_expr(src_cron_moy)
    src_cron_dow_list = get_vlaues_list_from_cron_expr(src_cron_dow)

    # List of converted Hours which are remaining in the same day
    same_day_hours = []

    # List of converted Hours which are touching previous/next day.
    different_day_hours = []

    # Get the Converted minutes list
    mins_list = []

    """
    Iterate through hours and minutes and get Converted Hours to Server TZ.
    Also determine if any convertion touching previous/next day.
    """
    if src_cron_hour_list and src_cron_min_list:
        for hour in src_cron_hour_list:
            for _min in src_cron_min_list:
                ms = hour * 60 * 60 * 1000 + _min * 60 * 1000

                src_dt = datetime.datetime.combine(datetime.datetime.today(), datetime.time(hour, _min))
                derived_dt = to_server_tz(src_dt, biz)

                hour_to_push = derived_dt.time().hour
                mins_to_push = derived_dt.time().minute

                if ms < diff_in_ms:
                    touched_previous_day = True
                    different_day_hours.append(hour_to_push)
                else:
                    in_same_day = True
                    same_day_hours.append(hour_to_push)
                mins_list.append(mins_to_push)

    # get Distinct values in every list
    different_day_hours = list(set(different_day_hours))
    same_day_hours = list(set(same_day_hours))
    mins_list = list(set(mins_list))

    """
    If touching previous day, Shift every DOW, MOY, DOM by 1 towards left.
    and make a cron expression using only the hours which are touching previous day
    """
    if touched_previous_day:
        prev_day_cron_expr_list = []
        moy_dom_set = []

        derived_cron_min_list = mins_list
        derived_cron_hour_list = different_day_hours

        # If `dow` is present (not *), then no option to present `moy`, `dom`
        if src_cron_dow_list:
            derived_cron_dow_list = left_shift_cron_expr_list(src_cron_dow_list, "dow")
            moy_dom_set.append( [ [], [] ] )
        else:
            derived_cron_dow_list = []

            # If first day of the month present then Conversion will touch last day of previous month
            # Devides the cron expressoin according to different possible Last day value of different month.
            if 1 in src_cron_dom_list:
                last_date_month_dict = dict()
                derived_cron_moy_list = left_shift_cron_expr_list(src_cron_moy_list, 'moy')
                for _m in derived_cron_moy_list:
                    _, last_date = calendar.monthrange(src_dt.year, _m)
                    if not last_date_month_dict.get(last_date):
                        last_date_month_dict[last_date] = []
                    last_date_month_dict[last_date].append(_m)

                for k, v in last_date_month_dict.items():
                    moy_dom_set.append([ list(set(v)), [k] ])

                moy_dom_set.append([ src_cron_moy_list,
                                     left_shift_cron_expr_list(list( set(src_cron_dom_list) - set([1]) ), "dom") ])
            else:
                moy_dom_set.append( [ src_cron_moy_list,  left_shift_cron_expr_list(src_cron_dom_list, "dom") ] )

        for derived_moy, derived_dom in moy_dom_set:
            prev_day_cron_expr_list.append(make_cron_expression_from_lists(derived_cron_dow_list, derived_moy,
                                                derived_dom, derived_cron_hour_list, derived_cron_min_list))
    else:
        prev_day_cron_expr_list = []

    """
    If NOT touching previous day, Same DOW, MOY, DOM Will be used.
    And Will make a cron expression using only the hours which are in the same day.
    """
    if in_same_day:
        same_day_cron_expr_list = [make_cron_expression_from_lists(src_cron_dow_list, src_cron_moy_list,
                                            src_cron_dom_list, same_day_hours, mins_list)]
    else:
        same_day_cron_expr_list = []

    return prev_day_cron_expr_list + same_day_cron_expr_list

