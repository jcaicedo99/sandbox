def cntMoves(arr : list):
    size = len(arr)
    
    copy = arr.copy()
    copy.sort()
    
    indices_dict = {k:v for k,v in zip(arr,range(size))}

    print(indices_dict)
    moves = 0
    
    for pos in range(size):
        
       if arr[pos] != copy[pos]:
           print(arr[pos], copy[pos])
           arr[indices_dict[arr[pos]]] = copy[pos]
           arr[pos] = copy[pos]
           print(arr) 
           moves += 1   
             
    
    print(arr)
    return moves
if __name__ == "__main__":
    print(cntMoves([8,6,7,5]) )