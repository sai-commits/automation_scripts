import pandas as pd
from common import BQManager
from common.wh_managers_utils import get_logger
from jobs.sai_test.queries_for_dataframe import PLAN_CASH_EXTRACTED, PLAN_COLLECTION_GENERATED, \
    COLLECTION_UNITS_OF_CASH, \
    ALL_PLAN_COLLECTIONS
import pandas as pd
from common import BQManager
from common.wh_managers_utils import get_logger
from jobs.sai_test.queries_for_dataframe import PLAN_CASH_EXTRACTED, PLAN_COLLECTION_GENERATED, \
    COLLECTION_UNITS_OF_CASH, \
    ALL_PLAN_COLLECTIONS


global_array = []
#line 10 - 39 Importing BQ data into pandas dataframes
def bq_to_data_frame(sql_query, column_list):
    bq_manager = BQManager(identity="")
    bq_data = []
    bq_data.extend(bq_manager.query_with_results(query=sql_query, project_id='data-gl',
                                                 use_legacy_sql=False))
    data_frame = pd.DataFrame(bq_data, columns=column_list)
    return data_frame


df_plan_cash = bq_to_data_frame(PLAN_CASH_EXTRACTED,
                                ['plan_id', 'cash_adjustment', 'failed_source_type', 'failed_source_id',
                                 'failed_plan_collection_unit_id', 'failed_plan_collection_id', 'source_type',
                                 'source_id', 'plan_cash_id'])
df_plan_coll_units = bq_to_data_frame(COLLECTION_UNITS_OF_CASH,
                                      ['source_id', 'source_type', 'plan_collection_id', 'amount_cents'])
df_plan_collections = bq_to_data_frame(ALL_PLAN_COLLECTIONS,
                                       ['source_id', 'source_type', 'plan_collection_id', 'amount_cents'])
df_plan_coll_units_generated = bq_to_data_frame(PLAN_COLLECTION_GENERATED,
                                                ['plan_id', 'amount_cents', 'source_type', 'source_id'])

#function scan through a dataframe and return row when matches
def row_scanner(dataFrame, scanColumn, scanColumnValue):
    reslt_df = dataFrame.loc[dataFrame[scanColumn] == scanColumnValue]
    return reslt_df

#main logic function which reapeats until source is not plancashadjustment and stores the source transaction in an array
def planCashIterator(plan_cash_id):
    test_df = row_scanner(df_plan_cash, 'plan_cash_id', plan_cash_id)
    if len(test_df) > 0:
        if test_df['failed_source_type'].values[0] == "PlanCashCreditAdjustment":
            planCashIterator(test_df['failed_source_id'].values[0])
        elif test_df['source_type'].values[0] == "PlanCashAdjustment":
            planCashIterator(test_df['source_id'].values[0])
        elif test_df['failed_plan_collection_id'].values[0] != None:
            iteration_df = row_scanner(df_plan_collections, 'plan_collection_id',
                                      test_df['failed_plan_collection_id'].values[0])

            for index, row in iteration_df.iterrows():
                if row['source_type'] == 'PlanCashAdjustment':
                    planCashIterator(row['source_id'])
                else:

                    global_array.append(
                        {
                            'plan_id': 'tbd',
                            'latest_generated_plan_cash_id': 'tbd',
                            'latest_generated_plan_cash_amount': 'tbd',
                            'source_id': row['source_id'],
                            'source_type': row['source_type'],
                            'amount_cents': row['amount_cents']

                        }
                    )

            return pd.DataFrame(global_array)
        else:
            source_id = test_df['source_id'].values[0] or test_df['failed_source_id'].values[0]
            source_type = test_df['source_type'].values[0] or test_df['failed_source_type'].values[0]
            amount_cents = test_df['cash_adjustment'].values[0]


            global_array.append({'plan_id': 'tbd',
                                 'latest_generated_plan_cash_id': 'tbd',
                                 'latest_generated_plan_cash_amount': 'tbd',
                                 'source_id': source_id, 'source_type': source_type, 'amount_cents': amount_cents})

        return pd.DataFrame(global_array)


#loops through  plancashadjustments that are in generated state to get to the source.
def main_function():
  try:
    df = pd.DataFrame([], columns=['plan_id', 'latest_generated_plan_cash_id', 'latest_generated_plan_cash_amount',
                                   'source_id', 'source_type', 'amount_cents'])

    for index, row in df_plan_coll_units_generated.iterrows():
        if row['source_type'] == 'PlanCashAdjustment':

            plan_id = row['plan_id']
            source_id = row['source_id']

            amount_cents = row['amount_cents']
            local_df = planCashIterator(row['source_id'])
            global_array.clear()

            if local_df is not None:
                local_df['plan_id'] = local_df['plan_id'].replace(['tbd'], plan_id)
                local_df['latest_generated_plan_cash_id'] = local_df['latest_generated_plan_cash_id'].replace(['tbd'],
                                                                                                              source_id)
                local_df['latest_generated_plan_cash_amount'] = local_df['latest_generated_plan_cash_amount'].replace(
                    ['tbd'], amount_cents)
                df = pd.concat([df, local_df])
        else:
            print(row['source_type'])
            check_df = df.loc[(df['source_id'] == row['source_id']) & (df['source_type'] == row['source_type'])]
            new_df = []
            if check_df.empty:
                new_df.append({'plan_id': row['plan_id'],
                           'latest_generated_plan_cash_id': 'None',
                           'latest_generated_plan_cash_amount': 'None',
                           'source_id': row['source_id'], 'source_type': row['source_type'],
                           'amount_cents': row['amount_cents']})
                print(new_df)
            df = pd.concat([df, pd.DataFrame(new_df)])
    df.to_csv(r'/Users/sai-gdl/Desktop/failed_ach.csv', header=True)

    return df
  except Exception as e:
      logger = get_logger(logger_name="Initial Segment Profiles Upload")
      logger.error("root transaction error".format(e))
      raise


print(main_function())

