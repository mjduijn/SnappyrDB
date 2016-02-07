


main = do  
	--"Get" variable
    xs <- getLine
	--Perform actions with xs
	--...
	--"Put" altered variable    
    putStrLn $ f1 xs

:t main
>> main :: IO ()


main = do 
	val1 <- get "Key1"
	--Perform actions with val1
	--...
	--"Put" altered variable  
	put "Key1" "new value 1"
	del "Key1"

:t main
>> main :: SnappyDB ()




snappy = SnappyDB
//Get value from SnappyDB and perform action with it
.get("Key1", action1)                                                            ;
//Put a new key-value pair in the database   
.put("Key1", val1)                                                          ;
//Delete a key-value pair
.del("Key1");

:t snappy
>> snappy :: SnappyDB
