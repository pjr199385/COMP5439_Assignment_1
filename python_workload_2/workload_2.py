from pyspark import SparkContext
import csv
import argparse

def extractRating_x(record):
    try:
        video_id, trending_date, category_id, category, publish_time, views, likes, dislikes, comment, count, video_error, country = record.split(",")
        if video_id == 'video_id':
            return ()
        year, day, month = trending_date.split('.')
        absolute_time = int(year)*1000 + int(month)*100 + int(day)
        dislike_minus_like = int(dislikes) - int(likes)
        return (video_id + country, (absolute_time, dislike_minus_like))
    except:
        return ()


def extractCategory(record):
    try:
        video_id, trending_date, category_id, category, publish_time, views, likes, dislikes, comment, count, video_error, country = record.split(",")
        if video_id == 'video_id':
            return ()
        year, day, month = trending_date.split('.')
        absolute_time = int(year)*1000 + int(month)*100 + int(day)
        return (video_id + country, (absolute_time, category))
    except:
        return ()


def mapFirstCat(line):
    movie_id , data = line
    category = data[0][1]
    return (movie_id, category)


def mapGrowth(line):
    movie_id , data = line
    if len(data) < 2:
        score = 0
    else:
        score = data[1][1] - data[0][1]
    return (movie_id, score)


def mapUnzipCountry(line):
    # ('BEePFpC9qG8DE', (366556, 'Film & Animation')
    movie_id_country , time_cat = line
    movie_id, country =  movie_id_country[:-2], movie_id_country[-2:]
    time, cat = time_cat
    return (movie_id, time, cat, country)


if __name__ == "__main__":
    sc = SparkContext(appName="Workload_2")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='')
    parser.add_argument("--output", help="the output path", 
                        default='') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    
    ratings = sc.textFile(input_path + "AllVideos_short.csv")
    videoCat = ratings.map(extractCategory)

    header = videoCat.first()
    videoCat = videoCat.filter(lambda line: line != header).sortBy(lambda a: a[1][0]).groupByKey().mapValues(list)
    videoCat = videoCat.map(mapFirstCat)

    videoData = ratings.map(extractRating_x)

    header = videoData.first()
    videoData = videoData.filter(lambda line: line != header)
    videoData = videoData.sortBy(lambda a: a[1][0])
    videoData = videoData.groupByKey().mapValues(list)
    videoData = videoData.map(mapGrowth)
    videoData = videoData.sortBy(lambda a: a[1], ascending= False)

    top_10_videoData = sc.parallelize(videoData.take(10))

    final_result = top_10_videoData.join(videoCat).sortBy(lambda a: a[1][0], ascending=False).map(mapUnzipCountry)
    final_result.saveAsTextFile(output_path)