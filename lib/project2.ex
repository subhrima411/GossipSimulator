#Each agent is linked to a spawned process.
#The agent of a process keeps track of the neighbors and counter in case of gossip algorithm.
#In case of push-sum, the agent holds its respective neighbors and {s,w} values.
defmodule AgentProcess do
  def start_link(neighbor, count, pidAtom) do
    Agent.start_link(fn -> [neighbor] ++ [count]  end, name: pidAtom) 
    Agent.get(pidAtom, &(&1))
  end
end

#This module has the main method and parses the arguments.
defmodule MainServer do
  def main(args) do
    args |> parse_args  
  end
    
  defp parse_args([]) do
    IO.puts "No arguments given. Please enter the number of nodes, topology and algorithm."
  end

  defp parse_args(args) do
    {_, topology, _} = OptionParser.parse(args)                  
        list =[]
        top = Enum.at(topology,1)
        if top == "line" || top == "full" || top == "2D" || top == "imp2D" do
            MainServer.spawnThreadsServer(0, 10, self(), list, topology) 
            keepAlive
        else
            IO.puts "Wrong topology!"
        end
  end

  def keepAlive do
    keepAlive
  end

  #This function spawns the required number of processes according to the numNodes parameter and adds each process to a list.
  #Once count index reaches numNodes, the respective topology creation function is called.
  def spawnThreadsServer(count, intx, pid, list, topology) do
    algorithm = Enum.at(topology, 2) 
    top = Enum.at(topology, 1)
    numNodes = String.to_integer(Enum.at(topology, 0))
  
    if count == numNodes do 
      startTime = :os.system_time(:millisecond)
      cond do
       top === "full"  -> createFull(list, 0, startTime, numNodes, algorithm)
       (top === "2D") || (top === "imp2D") -> power = :math.pow(trunc(Float.ceil(:math.sqrt(numNodes))),2);
                                                      spawnMoreProcessesForGrid(length(list), list, power, startTime, algorithm,top, numNodes)
       top == "line" -> createLine(list, 0, startTime, numNodes, algorithm)        
       true -> IO.puts "Wrong algorithm"
      end
    else 
      pid = spawn(MainServer, :receiveMsg, [])
      list = list ++ [pid]
      spawnThreadsServer(count + 1, intx, pid, list, topology)
    end
  end

  #This function handles the functionality of each process after it receives a message.
  #There are two cases, one for gossip and one for push-sum.
  #For push-sum, the receiver adds half of its {s,w} values to its current {s,w}
  def receiveMsg do
    receive do
      {pid, gossip, temp, startTime, numNodes, list} ->  pidName = List.to_atom(:erlang.pid_to_list(pid))
                                                         receiveMsg
                                                         
      {pid, sends, sendw} -> pidName = List.to_atom(:erlang.pid_to_list(pid)) ;  
                             current = Agent.get(pidName, fn state -> List.last(state) end);
                             newS = elem(current, 0) + sends;
                             newW = elem(current, 1) + sendw;
                             noOfRounds = elem(current, 2);
                             Agent.update(pidName, fn state -> List.replace_at(state, length(state) - 1, {newS, newW, noOfRounds}) end);
                             receiveMsg
    end
  end

  #Line topology is created with adjacent processes as neighbors.
  #For the first and last processes, only one neighbor exists
  #The agent is started for each process with neighbors and counter initialized to 0 for gossip and neighbors and {s,w} for push-sum.
  #After creation, a process is randomly picked by the main process. This process is the starting point for the gossip or push-sum algorithm. 
  def createLine(list, index, startTime, numNodes, algorithm) do  
  if index < length(list) do
     if(index == 0) do
        pid = Enum.at(list,0) 
        neighbor = Enum.at(list,1) 
        pidName = List.to_atom(:erlang.pid_to_list(pid))
        neighbors = {neighbor}
      end  
      if(index == length(list) - 1) do
        pid = Enum.at(list,length(list) - 1) 
        neighbor = Enum.at(list,length(list) - 2) 
        pidName = List.to_atom(:erlang.pid_to_list(pid))
        neighbors = {neighbor}
      end
      if(index > 0 && index < length(list) - 1) do
        pid = Enum.at(list, index)
        neighbor1 = Enum.at(list,index - 1)
        neighbor2 = Enum.at(list,index + 1)
        pidName = List.to_atom(:erlang.pid_to_list(pid))
        neighbors = {neighbor1, neighbor2}
      end
       cond do
        algorithm == "gossip" -> AgentProcess.start_link(neighbors,0, pidName)
        algorithm == "push-sum" -> AgentProcess.start_link(neighbors,{index+1, 1, 0}, pidName)
        true-> IO.inspect "Wrong algorithm!"
        end
      createLine(list, index + 1, startTime, numNodes, algorithm) 
  else
    pid = Enum.random(list)
     cond do
        algorithm == "gossip" -> send(pid, {pid, "gossip", 0, startTime, numNodes, list});
                                 sendGossip(pid,0,startTime, numNodes, list)
        algorithm == "push-sum" -> sendSW(list, pid,0,startTime, numNodes, 0)
        true-> IO.inspect "Wrong algorithm!"
     end 
  end
 end

#Full topology is created with every process as the neighbor except the process itself.
#The agent is started for each process with neighbors and counter initialized to 0 for gossip and neighbors and {s,w} for push-sum.
#After creation, a process is randomly picked by the main process. This process is the starting point for the gossip or push-sum algorithm. 
def createFull(list, index, startTime, numNodes, algorithm) do  
  if index < length(list) do
        pid = Enum.at(list,index)
        temp = list -- [pid]
        temp = List.to_tuple(temp)
        pidName = List.to_atom(:erlang.pid_to_list(pid))
        cond do
        algorithm == "gossip" -> AgentProcess.start_link(temp,0, pidName)
        algorithm == "push-sum" -> AgentProcess.start_link(temp,{index+1, 1, 0}, pidName)
        true-> IO.inspect "Wrong algorithm!"
        end
      createFull(list, index + 1, startTime, numNodes, algorithm) 
  end
  pid = Enum.random(list)
  IO.inspect "Selected pid: "
  IO.inspect pid
  cond do 
  algorithm == "gossip" -> send(pid, {pid, "gossip"});
                           sendGossip(pid,0, startTime, numNodes, list);
  algorithm == "push-sum" -> sendSW(list, pid,0,startTime, numNodes, 0)
  end
 end

#2D or imp2D topology is created by transforming the list index for each process to a 2D matrix index.
#The agent is started for each process with neighbors and its i,j values for gossip and neighbors and {i,j} and {s,w} for push-sum. 
def createGrid(list, index, startTime, power, i, j, mapMatrix, algorithm, top1,  numNodes) do  
  pow = :math.sqrt(power)
  if(index == length(list)) do
    getGridNeighbours(list, 0, startTime, power, mapMatrix, algorithm,top1, numNodes)
  end
  pid = Enum.at(list, index) 

  cond do
    index < pow ->  mapMatrix = Map.put(mapMatrix, {i,j}, pid);
                    pidName = List.to_atom(:erlang.pid_to_list(pid))
                    if (algorithm == "gossip") do AgentProcess.start_link({},{i,j}, pidName) end
                    if (algorithm == "push-sum") do AgentProcess.start_link({},[{i,j},{index + 1, 1, 0}], pidName) end
                    j = j + 1;
    (index >= pow && index <= length(list) - 1) -> if (rem(index,trunc(pow)) == 0) do 
                                                      j = 0
                                                      i = i + 1
                                                      pidName = List.to_atom(:erlang.pid_to_list(pid))
                                                      mapMatrix = Map.put(mapMatrix, {i,j}, pid);
                                                      if (algorithm == "gossip") do AgentProcess.start_link({},{i,j}, pidName) end
                                                      if (algorithm == "push-sum") do AgentProcess.start_link({},[{i,j},{index + 1, 1, 0}], pidName) end
                                                   else
                                                      j = j + 1
                                                      pidName = List.to_atom(:erlang.pid_to_list(pid))
                                                      mapMatrix = Map.put(mapMatrix, {i,j}, pid);
                                                      if (algorithm == "gossip") do AgentProcess.start_link({},{i,j}, pidName) end
                                                      if (algorithm == "push-sum") do AgentProcess.start_link({},[{i,j},{index + 1, 1, 0}], pidName) end
                                                   end
    true -> 
  end
  createGrid(list, index + 1, startTime, power, i, j, mapMatrix, algorithm, top1, numNodes)
end

#This function discovers neighbors for each process by looking at the index of each process stored by the respective agent
#For imp2D, an extra random neighbor is selected and added to the neighbor list
def getGridNeighbours(list, index, startTime, power, mapMatrix, algorithm, top1, numNodes) do
    if(index <= length(list)-1) do
      pid = Enum.at(list,index)
      pidName = List.to_atom(:erlang.pid_to_list(pid))  
      if(algorithm  == "push-sum") do actorRowColumn = Agent.get(pidName, fn state -> List.first(List.last(state)) end) end
      if(algorithm == "gossip") do actorRowColumn = Agent.get(pidName, fn state -> List.last(state) end) end
      row = elem(actorRowColumn, 0) 
      column = elem(actorRowColumn, 1)
      neighbor1 = Map.get(mapMatrix, {row, column+1})
      neighbor2 = Map.get(mapMatrix, {row, column-1})
      neighbor3 = Map.get(mapMatrix, {row-1, column})
      neighbor4 = Map.get(mapMatrix, {row+1, column})
      neighbors = {}

      if(neighbor1 != nil) do neighbors = Tuple.append(neighbors, neighbor1) end
      if(neighbor2 != nil) do neighbors = Tuple.append(neighbors, neighbor2) end
      if(neighbor3 != nil) do neighbors = Tuple.append(neighbors, neighbor3) end
      if(neighbor4 != nil) do neighbors = Tuple.append(neighbors, neighbor4) end

      if(top1 == "imp2D") do
         listValid = list -- Tuple.to_list(neighbors) 
         resultList = listValid -- [pid]
         randomNeighbor = Enum.random(resultList)
         neighbors = Tuple.append(neighbors, randomNeighbor)
      end
      Agent.update(pidName, fn state -> List.replace_at(state, 0, neighbors) end)
      if (algorithm == "gossip") do  Agent.update(pidName, fn state -> List.replace_at(state, 1, 0) end) ;
                                     end
      if (algorithm == "push-sum") do val = Agent.get(pidName, fn state -> List.last(state) end);
                                      val = val --[List.first(val)];
                                      finalVal = List.first(val);
                                      Agent.update(pidName, fn state -> List.replace_at(state, 1, finalVal) end); 
                                    end
      getGridNeighbours(list, index + 1, startTime, power, mapMatrix, algorithm, top1, numNodes)
      else
        IO.inspect "Created neighbors successfully! :)"
        pid = Enum.random(list)
        cond do 
          algorithm == "gossip" -> send(pid, {pid, "gossip"});
                                   pidName = List.to_atom(:erlang.pid_to_list(pid)); 
                                   sendGossip(pid,0,startTime, numNodes, list);
          algorithm == "push-sum" -> pidName = List.to_atom(:erlang.pid_to_list(pid));
                                     sendSW(list, pid,0, startTime, numNodes, 0)
        end
    end
end
    
#This function spawns extra processes for 2D and imp2D to make it a perfect square.
def spawnMoreProcessesForGrid(count, list, power, startTime, algorithm, top, numNodes) do
    if(count == power) do
      i = 0;
      j = 0;
      mapMatrix = %{}
      createGrid(list, 0, startTime, power, i, j, mapMatrix, algorithm, top, numNodes)
    else
      pid = spawn(MainServer, :receiveMsg, [])
      list = list ++ [pid]
      spawnMoreProcessesForGrid(count + 1, list, power, startTime, algorithm, top, numNodes)
    end
end

#This function sends gossip and keeps selecting a random neighbor until it hears the gossip 10 times.
#The convergence time is printed once all the processes hear the gossip 10 times.
def sendGossip(pid, temp, startTime, numNodes, list) do
     if length(list) == 0 do
       endTime = :os.system_time(:millisecond)
       convergenceTime = endTime - startTime
       IO.puts "Time taken for convergence is: #{convergenceTime} milliseconds"
       Process.exit(self(), :kill)
     end

     pidName = List.to_atom(:erlang.pid_to_list(pid))      
     selectedNeighbor = Agent.get(pidName, fn state -> Enum.random(Tuple.to_list(List.first(state))) end)   
     selectedNeighborName = List.to_atom(:erlang.pid_to_list(selectedNeighbor))   
     count = Agent.get(selectedNeighborName, fn state -> List.last(state) end)  
     
     cond do
        count == 0 -> IO.inspect selectedNeighbor; 
                      IO.inspect "Heard for first time"; 
                      Agent.update(selectedNeighborName, fn state -> List.replace_at(state, length(state) - 1, 1) end);
                      send(selectedNeighbor, {selectedNeighbor, "gossip", temp, startTime, numNodes, list});
                      sendGossip(selectedNeighbor,temp, startTime, numNodes,list)

        count > 0 && count <= 9 -> IO.inspect pid; IO.inspect " sending "; IO.inspect selectedNeighbor;
                                   Agent.update(selectedNeighborName, fn state -> List.replace_at(state, length(state) - 1, List.last(state) + 1) end);
                                   send(selectedNeighbor, {selectedNeighbor, "gossip", temp, startTime, numNodes, list});
                                   sendGossip(selectedNeighbor,temp,startTime, numNodes,list)

        count >= 10 -> IO.inspect pid; 
                       IO.inspect "Heard the gossip 10 times. Can't transmit anymore" ;                
                       list = list -- [pid]
                       sendGossip(selectedNeighbor,temp,startTime, numNodes,list)    
    end
  end

#This function implements the push-sum algorithm and keeps selecting a random neighbor until it converges.
#The convergence time and final convergence ratio is printed once all the processes are converged.
def sendSW(list, pid, temp, startTime, numNodes, sumCurrentRatio) do
     if length(list) == 0 do
       endTime = :os.system_time(:millisecond)
       convergenceTime = endTime - startTime
       convergenceRatio = sumCurrentRatio / numNodes
       IO.puts "The convergence ratio is: #{convergenceRatio}"
       IO.puts "Time taken for convergence is: #{convergenceTime} milliseconds"
       Process.exit(self(), :kill)
     end

    pidName = List.to_atom(:erlang.pid_to_list(pid))      
    selectedNeighbor = Agent.get(pidName, fn state -> Enum.random(Tuple.to_list(List.first(state))) end)  
    IO.inspect "Selected neighbor is: "
    IO.inspect selectedNeighbor

     noOfRounds = Agent.get(pidName, fn state -> elem(List.last(state),2) end)  
     if(noOfRounds >= 3) do
        IO.inspect pid
        IO.inspect "No. of rounds = 3. Process will now stop transmitting"
        if(Enum.member?(list, pid)) do
            currentRatio = Agent.get(pidName, fn state -> elem(List.last(state),0) end) / Agent.get(pidName, fn state -> elem(List.last(state),1) end)           
            sumCurrentRatio = sumCurrentRatio + currentRatio
        end
        list = list -- [pid]
        sendSW(list, selectedNeighbor, temp, startTime, numNodes, sumCurrentRatio)
     end

     selectedNeighbor = Agent.get(pidName, fn state -> Enum.random(Tuple.to_list(List.first(state))) end)   
     selectedNeighborName = List.to_atom(:erlang.pid_to_list(selectedNeighbor))    
     previousRatio = Agent.get(pidName, fn state -> elem(List.last(state),0) end) / Agent.get(pidName, fn state -> elem(List.last(state),1) end)    
     sends = Agent.get(pidName, fn state -> elem(List.last(state),0)/2 end)  
     sendw = Agent.get(pidName, fn state -> elem(List.last(state),1)/2 end)       
     Agent.update(pidName, fn state -> List.replace_at(state, length(state) - 1, {sends, sendw, noOfRounds}) end)
     currentRatio = Agent.get(pidName, fn state -> elem(List.last(state),0) end) / Agent.get(pidName, fn state -> elem(List.last(state),1) end)           
     
     if(abs(currentRatio-previousRatio) < :math.pow(10,-10)) do
        noOfRounds = noOfRounds + 1
        Agent.update(pidName, fn state -> List.replace_at(state, length(state) - 1, {sends, sendw, noOfRounds}) end)
     else
      Agent.update(pidName, fn state -> List.replace_at(state, length(state) - 1, {sends, sendw, 0}) end)
     end
    
    send(selectedNeighbor, {selectedNeighbor, sends, sendw})
    sendSW(list, selectedNeighbor, temp, startTime, numNodes, sumCurrentRatio)
 end  
end