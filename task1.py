from pyspark import SparkConf
from pyspark.context import SparkContext
import sys
import itertools
import time
import copy

def tofile(final_candidate_list):
    l = 1
    first = 1
    for mem in final_candidate_list:
        tem = len(mem)
        if tem == 1:
            if first == 1:
                ayo = "('" + mem[0] + "'"
                fh.write(ayo)
                first = 0
                continue
            fh.write("),")
            ayo = "('" + mem[0] + "'"
            fh.write(ayo)
            continue
        if tem == l:
            fh.write("),")
            fh.write("(")
            for i in range(l):
                if i != l - 1:
                    ayo = "'"+mem[i]+ "', "
                    fh.write(ayo)
                else:
                    ayo = "'" + mem[i] + "'"
                    fh.write(ayo)
        else:
            l = tem
            fh.write(')')
            fh.write('\n\n')
            fh.write("(")
            for i in range(l):
                if i != l - 1:
                    ayo = "'" + mem[i] + "', "
                    fh.write(ayo)
                else:
                    ayo = "'" + mem[i] + "'"
                    fh.write(ayo)
    fh.write(")\n\n")

def unique(line):
    # intilize a null list
    list = line[1]
    unique_list = []
    # traverse for all elements
    for x in list:
        # check if exists in unique_list or not
        if x not in unique_list:
            unique_list.append(x)
    x = (line[0],unique_list)
    return x

def combo_function(candidatelist):

    length_of_candidate = len(candidatelist)
    length_of_each = len(candidatelist[0])
    potential_candidates = []

    for i in range(length_of_candidate-1):
        for j in range(i+1,length_of_candidate):
            if candidatelist[i][:-1] == candidatelist[j][:-1]:
                temp = tuple(sorted(list(set(candidatelist[i]).union(set(candidatelist[j])))))
                temp_subsets=[]
                for subset in itertools.combinations(temp,length_of_each):
                    temp_subsets.append(subset)
                if(set(temp_subsets).issubset(set(candidatelist))):
                    potential_candidates.append(temp)
    return(potential_candidates)

def PCY_algorithim(chunk1, total_length, s):
    count_table = {}
    hash_count = {}
    hash_bitmap = {}
    frequent_item_list = []
    candidates = []
    list1 = []
    chunk = copy.deepcopy(chunk1)
    chunk_length = len(chunk)
    p = chunk_length/total_length
    threshold = p*s
    q = 0
    #hash_bitmap is initialized to 0 for bucket number of entries
    for entry in range(num_buckets):
        hash_bitmap[entry] = 0
    #Chunk is now a deepcopy of original chunk so all baskets in it can be played with
        #Count of all the individual items in all baskets
        #Generate pairs of 2 for each basket. Generate hash for this pair 2 and increase count of each bucket Bitmap
        #Bucket count checked for threshold and original bitmap (hash_bitmap) created
    for basket3 in chunk:
        for item in basket3:
            count_table[item]=count_table.get(item,0)+1
        for pair2 in itertools.combinations(basket3, 2):
            subset_list = list(pair2)
            templist = []
            for word in subset_list:
                for c in word:
                    num = ord(c)
                    templist.append(num)
            total = sum(templist)
            #total = sum(map(lambda x: int(x), list(pair2)))
            index = total % num_buckets
            hash_count[index] = hash_count.get(index,0)+1
        for entry in hash_count:
            if hash_count[entry] >= threshold:
                hash_bitmap[entry] = 1

    #print("PCY Stage1 done")

    #Frequent item list created having single Eg: '100' '101' etc which are the ones to be considered.

    for entry in count_table:
        if count_table[entry] >= threshold:
            frequent_item_list.append(entry)
    frequent_item_list = sorted(frequent_item_list)

    #Adding all these single value frequent items to the candidate_all list
    potential_candidates = frequent_item_list
    for single in frequent_item_list:
        apd = (tuple([single]),1)
        candidates.append(apd)
    short_chunk = []

    for basket_to_check in chunk:
        copy_basket = copy.deepcopy(basket_to_check)
        for item in basket_to_check:
            if item not in frequent_item_list:
                copy_basket.remove(item)
        short_chunk.append(copy_basket)

    num_pairs = 1
    loop_next = True
    while loop_next is True:
        num_pairs = num_pairs + 1
        frequent_subset_count = {}
        chaaa = []

        for copy_basket in short_chunk:
            if len(copy_basket) >= num_pairs:
                if num_pairs >= 3:
                    for eln in potential_candidates:
                        if(set(eln).issubset(set(copy_basket))):
                            frequent_subset_count[eln]= frequent_subset_count.get(eln, 0) + 1
                else:
                    for subset in itertools.combinations(copy_basket, 2):
                        sorting_subset = tuple(sorted(subset))
                        subset_list = list(subset)
                        templist = []
                        for word in subset_list:
                            for c in word:
                                num = ord(c)
                                templist.append(num)
                        total = sum(templist)
                        #total = sum(map(lambda x: int(x), subset_list))
                        index = total % num_buckets
                        if hash_bitmap[index] == 1:
                            frequent_subset_count[sorting_subset] = frequent_subset_count.get(sorting_subset, 0) + 1
        for s in frequent_subset_count:
            if frequent_subset_count[s] >= threshold:
                chaaa.append(s)
        ne = sorted(chaaa)
        input_to_combo_function = copy.deepcopy(ne)  #try to optimise
        for q in input_to_combo_function:
            candidates.append((q,1))
        if len(frequent_subset_count) == 0:
            break
        if ((len(input_to_combo_function) > 0) and (input_to_combo_function is not None)):
            potential_candidates = combo_function(input_to_combo_function)
        loop_next = (len(potential_candidates) > 0) and (potential_candidates is not None)
    return (candidates)

def SON_Phase2(chunk,candidate_list):
    count_dict = {}
    for basket2 in chunk:
        for items in candidate_list:
            flag = 0
            for each_item in items:
                if each_item in basket2:
                    continue
                else:
                    flag = 1
            if flag == 1:
                continue
            abc = tuple(items)
            count_dict[abc] = count_dict.get(abc,0)+1
    count_tuple = count_dict.items()
    return (count_tuple)


if __name__ == "__main__":
    start = time.time()
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    case_number = int(sys.argv[1])
    s = int(sys.argv[2])
    input_file = sys.argv[3]
    output_file = sys.argv[4]
    num_buckets = 5
    n_partitions = 2

    fh = open(output_file,'w')
    dataRDD = sc.textFile(input_file)
    header = dataRDD.first()
    dataRDD = dataRDD.filter(lambda row: row != header).persist()

    if case_number == 1:
        basketRDD = dataRDD.map(lambda x : x.strip().split(",")).map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b).map(unique)
    elif case_number == 2:
        basketRDD = dataRDD.map(lambda x: x.strip().split(",")).map(lambda x: (x[1], [x[0]])).reduceByKey(lambda a, b: a + b).map(unique)
    else:
        print("INVALID CASE NUMBER : It has to be either 1 or 2")

    basketRDD = basketRDD.partitionBy(n_partitions).persist()

    total_data_length = basketRDD.count()
    p = basketRDD.getNumPartitions()
    data = basketRDD.values()
    a = data.collect()

    candidateitemsets = data.mapPartitions(lambda x: PCY_algorithim(list(x),total_data_length,s)).reduceByKey(lambda x, y: x+y)
    candidate = candidateitemsets.collect()
    output_list = []
    final_candidate_list = {}

    for i in range(len(candidate)):
        sorted_tuple = sorted(candidate[i][0])
        output_list.append(sorted_tuple)
    final_candidate_list = sorted(output_list)
    final_candidate_list.sort(key=len)
    fh.write('Candidates:\n')
    tofile(final_candidate_list)
    freqitemsets = data.mapPartitions(lambda x: SON_Phase2(list(x), final_candidate_list)).reduceByKey(lambda x, y: x+y).filter(lambda x : x[1]>=s).collect()
    output_list = []
    final_freq_list = {}
    for i in range(len(freqitemsets)):
        output_list.append(freqitemsets[i][0])
    final_freq_list = sorted(output_list)
    final_freq_list.sort(key=len)

    fh.write('Frequent Itemsets:\n')
    tofile(final_freq_list)
    fh.close()
    end = time.time()
    print("Duration: ", str(end - start))