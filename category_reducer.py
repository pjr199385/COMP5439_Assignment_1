 #!/usr/bin/python3

import sys


def read_map_output(file):
    for line in file:
        yield line.strip().split("\t", 1)


def category_reducer():
    country_count = set()
    video_count = set()
    current_category = ""
    for category, vnc in read_map_output(sys.stdin):
        video_id = vnc[0:-2]
        if current_category != category:
            if current_category !="":
                division_1 = len(country_count)
                division_2 = len(video_count)
                if division_2 == 0:
                    result = 0.0
                else:
                    result = round((float(division_1) / float(division_2)), 2)
                print("{}:{}".format(current_category, result))
            current_category = category
            country_count = set()
            video_count = set()
        country_count.add(vnc)
        video_count.add(video_id)
    if current_category != "":
        division_1 = len(country_count)
        division_2 = len(video_count)
    if division_2 == 0:
        result = 0.0
    else:
        result = round((float(division_1) / float(division_2)), 2)
    print("{}:{}".format(current_category, result))

    
if __name__ == "__main__":
    category_reducer()
