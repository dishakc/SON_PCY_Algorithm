from pyspark import SparkConf
from pyspark.context import SparkContext
import sys
import json
import csv

if __name__ == "__main__":
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    review_file = sys.argv[1]
    business_file = sys.argv[2]
    output_file = sys.argv[3]

    filter = "NV"

    reviewRDD = sc.textFile(review_file)
    reviewRDD = reviewRDD.map(lambda row: (json.loads(row)))
    businessRDD = sc.textFile(business_file)
    businessRDD = businessRDD.map(lambda row: (json.loads(row)))

    businessRDD = businessRDD.filter(lambda x: x['state'] == filter )

    business_list = businessRDD.map(lambda x: (x['business_id'])).collect()
    rRDD = reviewRDD.map(lambda x: (x['user_id'], x['business_id'])).filter(lambda x: x[1] in business_list)
    rdd = rRDD.collect()

    with open(output_file, 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(['user_id', 'business_id'])
        for i in rdd:
            spamwriter.writerow(i)


    #preprocess.py review.json business.json output.csv