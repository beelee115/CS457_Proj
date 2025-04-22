import multiprocessing
import random
import numpy as np
import time
import os
from dask.distributed import Client, LocalCluster

# Similar to GeeksforGeeks implementation
# https://www.geeksforgeeks.org/merge-sort/
def merge(arr, left, mid, right):
    """Merge two sorted subarrays"""
    # calculate sizes of left and right subarrays
    s1 = mid - left + 1
    s2 = right - mid
    
    # create temp arrays and copy data to them (L[] and R[])
    L = []
    for i in range(s1):
        L.append(arr[left+i])
    
    R = []
    for j in range(s2):
        R.append(arr[mid+1+j])
    
    # initialize index of first subarray and second subarray
    i = j = 0
    # index of merged subarray
    k = left
    
    # merge the temp arrays into arr[left right]
    while i < len(L) and j < len(R):
        if L[i] <= R[j]:
            arr[k] = L[i]
            i += 1
        else:
            arr[k] = R[j]
            j += 1
        k += 1
    
    # copy remaining elements of L and R
    while i < len(L):
        arr[k] = L[i]
        i += 1
        k += 1
    
    while j < len(R):
        arr[k] = R[j]
        j += 1
        k += 1

def mergeSort(arr, client, chunkSize=10_000):
    import dask.array as da
    import numpy as np

    # convert to NumPy array first
    arr_np = np.array(arr)
    # create Dask array 
    darr = da.from_array(arr_np, chunks=(chunkSize,))

    # sort the chunks
    sortedChunks = darr.map_blocks(np.sort, dtype=arr_np.dtype)

    # comput sorted chunks
    future = client.compute(sortedChunks)
    sortedParts = client.gather(future)

    # Ensure all parts are proper 1D arrays
    sortedParts = [np.atleast_1d(part) for part in sortedParts]

    # debug print 
    #print(f"Number of sorted parts: {len(sortedParts)}")
    #for i, part in enumerate(sortedParts[:3]):
     #   print(f"Part {i} shape: {np.array(part).shape}")

    # concatenate the parts and sort 
    combined = np.concatenate(sortedParts)
    finalSorted = np.sort(combined)

    return finalSorted.tolist()

def generateTestDataSet():
    # generate 100000 points to test merge sort on
    unsortedArr = []
    for _ in range(100_000):
        num = random.randint(0, 1_000_000)
        unsortedArr.append(num)

    with open('unsortedNum.txt', 'w') as f:
        f.write('\n'.join(map(str, unsortedArr)))
    return unsortedArr

def loadTestData():
    try:
        with open('unsortedNum.txt', 'r') as f:
            return [int(line.strip()) for line in f if line.strip()]
    except FileNotFoundError:
        return generateTestDataSet()
    
# verify if sorted correctly
def verifySort(arr):
    return np.all(np.diff(arr) >= 0)

def textOutput(sortedArr, filename='sortedOutput.txt'):
    with open(filename, 'w') as f:
        f.write('\n'.join(map(str, sortedArr)))
    print(f"\nSorted output saved")

if __name__ == "__main__":
    os.makedirs('output', exist_ok=True)
    # Test cases
    testArray = loadTestData()

    cluster = LocalCluster(n_workers=4, threads_per_worker=1)  # Local testing
    client = Client(cluster)

    # time parallel sort
    startTime = time.time()
    sortedArr = mergeSort(testArray, client)
    totalTime = time.time() - startTime
    print(f"Sort completed in {totalTime:.2f} seconds")
    print(f"Sort verified: {verifySort(sortedArr)}")

    textOutput(sortedArr, 'output/mergeSort.txt')
    
    client.close()
    cluster.close()