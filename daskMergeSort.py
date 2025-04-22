import numpy as np
import random
import time
from dask.distributed import Client, LocalCluster
import dask.array as da
from dask import delayed

def parallelMerge(left, right):
    # merge two sorted arrays into one sorted array
    result = np.empty(len(left) + len(right), dtype=left.dtype)
    i = 0
    j = 0
    k = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            result[k] = left[i]
            i += 1
        else:
            result[k] = right[j]
            j += 1
        k += 1
    
    # Copy remaining elements
    if i < len(left):
        result[k:] = left[i:]
    elif j < len(right):
        result[k:] = right[j:]
    return result

# distributed merge sort using dask
def parallelMergeSort(arr, client, chunkSize=10000):
    arrNp = np.array(arr)
    
    # create dask array with chunk size
    darr = da.from_array(arrNp, chunks=(chunkSize,))
    
    # sort each chunk in parallel
    sortedChunks = darr.map_blocks(np.sort, dtype=arrNp.dtype)
    
    # convert to delayed objects for tree reduction
    # get the delayed objects from the Dask array
    chunkDelayedObjects = sortedChunks.to_delayed()
    # flatten in case we have multi-dimensional chunks
    flatDelayedChunks = chunkDelayedObjects.flatten()
    # Convert to list of delayed objects
    delayedChunks = [delayed(chunk) for chunk in flatDelayedChunks]
    
    # tree reduction for merging
    while len(delayedChunks) > 1:
        newChunk = []
        for i in range(0, len(delayedChunks), 2):
            if i+1 < len(delayedChunks):
                # merge pairs of chunks
                merged = delayed(parallelMerge)(delayedChunks[i], delayedChunks[i+1])
                newChunk.append(merged)
            else:
                # odd number case - carry forward the last chunk
                newChunk.append(delayedChunks[i])
        delayedChunks = newChunk
    
    # compute the final merged array
    finalArr = client.compute(delayedChunks[0])
    return finalArr.result()

def verifySort(arr):
    # helper function to verify if dask merge sort works
    return np.all(np.diff(arr) >= 0)

def generateTestData(size=100000):
    # generate random numbers to sort
    return [random.randint(0, 1000000) for _ in range(size)]

if __name__ == "__main__":
    # setup cluster and the client
    cluster = LocalCluster(n_workers=4, threads_per_worker=1)
    client = Client(cluster)
    
    # generate the test data
    test_array = generateTestData()
    
    # run parallel merge sort and keep note of time
    startTime = time.time()
    sorted_arr = parallelMergeSort(test_array, client)
    totalTime = time.time() - startTime
    
    # output
    print(f"Sort completed in {totalTime:.2f} seconds")
    print(f"Sort verified: {verifySort(sorted_arr)}")
    
    # close client and cluster after we are done
    client.close()
    cluster.close()