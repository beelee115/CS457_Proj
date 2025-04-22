import multiprocessing
import random
import numpy as np
import time
import os

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

def mergeSort(arr, left, right, depth, max_depth):
    """Worker function that handles both parallel and sequential cases"""
    if left >= right:
        return
    
    mid = (left + right) // 2
    
    if depth < max_depth:
        # create left and right processes for a parallel execution
        left_proc = multiprocessing.Process(
            target=mergeSort,
            args=(arr, left, mid, depth+1, max_depth)
        )
        right_proc = multiprocessing.Process(
            target=mergeSort,
            args=(arr, mid+1, right, depth+1, max_depth)
        )
        # start processes
        left_proc.start()
        right_proc.start()
        
        #wait until completed
        left_proc.join()
        right_proc.join()
    else:
        # Sequential processing
        mergeSort(arr, left, mid, depth+1, max_depth)
        mergeSort(arr, mid+1, right, depth+1, max_depth)
    
    merge(arr, left, mid, right)

def parallelMergeSort(arr):
    # create shared array
    sharedArr = multiprocessing.Array('i', arr)
    
    # Start sorting with depth control
    mergeSort(sharedArr, 0, len(sharedArr)-1, 0, 2)
    
    return list(sharedArr)

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

    # time parallel sort
    startTime = time.time()
    sortedArr = parallelMergeSort(testArray.copy())
    totalTime = time.time() - startTime
    print(f"Parallel sort completed in {totalTime:.2f} seconds")
    print(f"Sort verified: {verifySort(sortedArr)}")

    textOutput(sortedArr, 'output/parallelSort.txt')
    
    #for original in test_arrays:
     #   print(f"\nOriginal array: {original}")
        
        # Sort and time
      #  sorted_arr = parallelMergeSort(original)
        
      #  print("Sorted array: ", sorted_arr)
      #  print("Expected:     ", sorted(original))
       # print("Correct?", sorted_arr == sorted(original))