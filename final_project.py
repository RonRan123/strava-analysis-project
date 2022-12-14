import pandas, re, time, pytz, datetime
import heartpy as hp
from fitness_tracker_data_parsing import get_dataframes

strava_folder = './strava_ronith/'
strava_activities = './strava_ronith/activities/'

POINTS_COLUMN_NAMES = ['latitude', 'longitude', 'lap', 'altitude', 'timestamp', 'heart_rate', 'cadence', 'speed']

est = pytz.timezone('US/Eastern')
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S %Z%z'

def read_activities_csv():
    activities_csv_file = strava_folder + 'activities.csv'
    df = pandas.read_csv(activities_csv_file)
    # Only keep the pertinent columns, drop any rows where there are null values (esp for the filename column)
    clean_df = df[['Activity ID', 'Activity Date', 'Activity Type', 'Elapsed Time',	'Distance', 'Filename']].dropna()
    # print(clean_df)
    # Only keep data that is for Run activity and the elapsed time is greater than or equal to 100
    run_df = clean_df[(clean_df['Activity Type'] == 'Run') & (clean_df['Elapsed Time'] >= 100)]
    # print(run_df)
    return run_df


def filter_by_date(df, start, end):
    # Convert object data type for Activity Date into datetime objecct
    df['Activity Date'] = pandas.to_datetime(df['Activity Date'], format='%b %d, %Y,  %H:%M:%S %p')
    filtered_df = df.loc[(df['Activity Date'] >= start) & (df['Activity Date'] < end)]
    return filtered_df


def df_to_csv(df, name):
    file_name = f'{name}.csv'
    file = open(file_name, 'w+')
    file.close()
    df.to_csv(file_name, encoding='utf-8', index=False, header=True)


def get_historical_weather(df):
    start_lat, start_long, start_timestamp = df.iloc[0]['latitude'], df.iloc[0]['longitude'], df.iloc[0]['timestamp']
    end_lad, end_long, end_timestamp = df.iloc[-1]['latitude'], df.iloc[-1]['longitude'], df.iloc[-1]['timestamp']
    print('start', time.mktime(start_timestamp.timetuple()), datetime_to_EST(start_timestamp))
    print('end', time.mktime(end_timestamp.timetuple()), end_timestamp.astimezone(est).strftime(fmt))


def datetime_to_EST(dt):
    return dt.astimezone(est).strftime(fmt)


def main():
    run_data = read_activities_csv()
    # Only focused on runs that occurred this semester (Fall 2022)
    filter_run_data = filter_by_date(run_data, 'Aug 20, 2022, 12:00:00 AM', 'Dec 31, 2022, 11:59:59 PM')
    filter_run_data.reset_index()
    # print(filter_run_data)
    regex_file = re.compile("^.+\.fit")
    for index, row in filter_run_data.iterrows():
        id, date, activity_type, time, distance, filename = row
        fn = regex_file.findall(filename)
        fit_file = strava_folder + fn[0]

        laps, points = get_dataframes(fit_file)
        print(points.info())
        clean_points = points.drop('altitude', axis=1)
        clean_points = clean_points.dropna()

        clean_points.reset_index()
        print(clean_points)
        file_number = re.findall(r'\d+', fn[0])[0]
        df_to_csv(clean_points, str(file_number))
        get_historical_weather(clean_points)
        break

if __name__ == "__main__":
    main()