import pandas as pd
import csv
import multiprocessing

input_files = ['data/clicks/part-001.csv','data/clicks/part-002.csv','data/clicks/part-003.csv','data/clicks/part-004.csv']

def mapper(file_path):
    loop_index = False
    with open(file_path) as reader, open('temp/map_rez.txt', 'a') as output:
        for row in reader:
            key = row.split(',')
            # Skipping first itteration since first row in data files are headers.
            if loop_index:
                output.write(key[0] +"\t"+ '1' +'\n')
            loop_index = True
    print('Mapping succesfull!')

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
        loop_index = False
        for row in reader:
            key = row.split()
            if key[0] == current_key:
                count += 1
            else:
                if loop_index:
                    output.write(str(current_key) +' '+ str(count)+ '\n')
                count = 0
                current_key = key[0]
                count += 1
            loop_index = True
        output.write(str(current_key) +' '+ str(count)+ '\n')

def writeToCsv():
    header = ['date', 'count']
    with open('data/total_clicks/Clicks_per_day2.csv', 'w', newline='') as file, open('temp/data_clicks') as reader:
        writer = csv.writer(file)
        writer.writerow(header)
        for row in reader:
            row = row.split()
            writer.writerow(row)
    print('Succesfully written')

if __name__ == '__main__':

    pool = multiprocessing.Pool(processes=4)

    for file_path in input_files:
        pool.apply(mapper, args=(file_path,))
    pool.close()
    pool.join()

    sort_map()
    reducer()

    writeToCsv()

