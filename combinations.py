

# This file contains the code for the combination sum 2 problem, potentially used 
# for fifth round matching 

def combinationSum2(candidates, target):
        # Sorting is really helpful, se we can avoid over counting easily
        candidates.sort()                      
        result = []
        combine_sum_2(candidates, 0, [], result, target)
        return result
        
def combine_sum_2(nums, start, path, result, target):
    # Recursion 
    
    # Base case: if the sum of the path satisfies the target, we will consider 
    # it as a solution, and stop there
    if not target:
        result.append(path)
        return
    
    if len(result) > 0:
        return 
    
    for i in range(start, len(nums)):

        if len(result) > 0: # OPTIMIZATION - IF WE FIND ONE COMBINATION, WE CAN RETURN 
            return 
        if i > start and nums[i] == nums[i - 1]:
            continue

        if nums[i] > target:
            break

        combine_sum_2(nums, i + 1, path + [nums[i]], 
                        result, target - nums[i])
        

def combinationSum3(candidates, target):
    # Stack
    candidates.sort()
    result = []
    stack = [(0, 0, [])]  # (current_sum, start_index, current_path)
    found = False
    while stack:
        current_sum, start, path = stack.pop()

        if current_sum == target:
            result.append(path)
            found = True
            return result 

        for i in range(start, len(candidates)):
            if found:
                return result
            if i > start and candidates[i] == candidates[i - 1]:
                continue

            new_sum = current_sum + candidates[i]
            if new_sum > target:
                break

            stack.append((new_sum, i + 1, path + [candidates[i]]))

    return result


def combinationSum4(candidates, target):
    # Dynamic Programming 
    candidates.sort()
    dp = [[] for _ in range(target + 1)]
    dp[0] = [[]]  # Base case: one way to make zero is to use an empty list

    for num in candidates:
        for t in range(target, num - 1, -1):
            for comb in dp[t - num]:
                new_comb = comb + [num]
                if new_comb not in dp[t]:
                    dp[t].append(new_comb)
                    return dp[target]

    return dp[target]


def can_sum(nums, target):
    possible_sums = set([0])
    
    for num in nums:
        if not isinstance(num, int):  # Ensure num is an integer
            continue
        new_sums = set()
        for s in possible_sums:
            new_sum = s + num
            if new_sum == target:
                return True
            if new_sum < target:
                new_sums.add(new_sum)
        possible_sums.update(new_sums)
    
    return target in possible_sums

# Example usage:
# nums = [2, 3, 7, 8, 10]
# target = 11
# print(can_sum(nums, target))  # Output: True

# test = [27,19,1000,50,81,50,158,100,10,1,108,51,11,40,60,50,10,100,100,5,100,10,130,3,97,100,100,117,200,1,82]
# print(combinationSum3(test, 500))

# test = [71.0, 9.0, 10.0, 15.0, 25.0, 60.0, 1.0, 1.0, 28.0, 185.0, 2.0, 8.0, 4000.0, 96.0, 1.0, 5.0, 26.0, 20.0, 18.0, 61.0, 2.0, 6.0, 20.0, 33.0, 8.0, 10.0, 9.0, 1.0, 1.0, 10.0, 63.0, 8.0, 51.0, 2120.0, 917.0, 985.0, 2000.0, 597.0, 800.0, 918.0, 1237.0, 250.0, 300.0, 645.0, 930.0, 1095.0, 1800.0, 2127.0, 2500.00, 38.0, 1.0, 33.0, 20.0, 66.0, 96.0, 25.0, 7.0, 5.0, 63.0, 11.0, 32.0, 70.0, 21.0, 20.0, 8.0, 3.0, 62.0, 32.0, 10.0, 5.0, 693.0, 1639.0, 2354.0, 4982.0, 73.0, 77.0, 332.0, 380.0, 380.0, 600.0, 600.0, 600.0, 636.0, 1324.0, 4565.0, 170.0, 10.0, 14.0, 26.0, 6.0, 23.0, 6.0, 15.0, 4.0, 9.0, 25.0, 2.0, 1.0, 75.0, 17.0, 32.0, 115.0, 101.0, 20.0, 61.0, 10.0, 97.0, 105.0, 130.0, 168.0, 80.0, 80.0, 81.0, 82.0, 87.0, 90.0, 13.0, 67.0, 10.0, 4.0, 10.0, 1.0, 1.0, 25.0, 63.0, 5.0, 10.0, 57.0, 1.0, 15.0, 9.0, 1.0, 51.0, 66.0, 93.0, 3.0, 38.0, 72.0, 40.0, 2.0, 10.0, 10.0, 5.0, 44.0, 27.0, 36.0, 64.0, 59.0, 10.0, 31.0, 31.0, 69.0, 10.0, 18.0, 5.0, 20.0, 17.0, 62.0, 13.0, 25.0, 1.0, 30.0, 10.0]
# target = 2880
# print(combinationSum3(test, target))
