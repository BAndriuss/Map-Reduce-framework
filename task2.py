import pandas as pd
import csv
import multiprocessing

input_files = ['data/clicks/part-001.csv','data/clicks/part-002.csv','data/clicks/part-003.csv','data/clicks/part-004.csv']
user_files = ['data/users/part-001.csv','data/users/part-002.csv']

def user_mapper(file_path):
    with open(file_path) as reader, open('temp/country.txt', 'a') as output:
        for row in reader:
            key = row.split(',')
            if 'LT\n' in key:
                output.write(key[0]+'\n')

def count_mapper(file_path):
    with open('temp/map_rez.txt', 'a') as output:
        reader = pd.read_csv(file_path)

        with open('temp/country.txt') as LT_users:
            users = list(line.strip() for line in LT_users)
        for index, row in reader.iterrows():
            if str(row['user_id']) in users:
                output.write(row[0] +'\t' + '1' +'\n')


def sort_map():
    with open('temp/map_rez.txt') as reader, open('temp/sort_rez.txt', 'w') as output:
        sorted_lines = sorted(reader)
        for line in sorted_lines:
            output.write(line)
    print('Sorted succesfully!')

def reducer():
    with open('temp/sort_rez.txt') as reader, open('temp/data_clicks', 'w') as output:
        current_key = None
        count = 0
        acceptFirst = False
        for row in reader:
            key = row.split()
            if key[0] == current_key:
                count += 1
            else:
                if acceptFirst:
                    output.write(str(current_key) +' '+ str(count)+ '\n')
                count = 0
                current_key = key[0]
                count += 1
            acceptFirst = True
        output.write(str(current_key) +' '+ str(count)+ '\n')

def writeToCsv():
    header = ['date', 'count']
    with open('data/total_clicks/Filtered_clicks.csv', 'w', newline='') as file, open('temp/data_clicks') as reader:
        writer = csv.writer(file)
        writer.writerow(header)
        for row in reader:
            row = row.split()
            writer.writerow(row)
    print('Succesfully written')

if __name__ == '__main__':

    for user_file in user_files:
        user_mapper(user_file)

    pool = multiprocessing.Pool(processes=4)
    for file_path in input_files:
        pool.apply(count_mapper, args=(file_path,))
    pool.close()
    pool.join()

    sort_map()
    reducer()

    writeToCsv()