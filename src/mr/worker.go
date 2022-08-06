/*
 * @Author: Shawshank980924 Akatsuki980924@163.com
 * @Date: 2022-07-30 03:04:24
 * @LastEditors: Shawshank980924 Akatsuki980924@163.com
 * @LastEditTime: 2022-08-04 13:41:18
 * @FilePath: /MIT6.824/src/6.824/src/mr/worker.go
 * @Description:
 *
 * Copyright (c) 2022 by Shawshank980924 Akatsuki980924@163.com, All Rights Reserved.
 */
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//worker函数通过调用rpc得到任务类型是mapf还是reducef，并确定输入文件的名称以及reducen的值
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	ApplyForTask(mapf ,reducef)


}

func ApplyForTask(mapf func(string, string) []KeyValue,
reducef func(string, []string) string){
	ApplyForTask: args := ApplyForTaskArgs{}
	args.ReadyForApply = true
	reply := ApplyForReply{}
	


	ok := call("Coordinator.ApplyForTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
		R := reply.TotReduceTasks
		index :=reply.TaskNumber
		M := reply.TotMapTasks
		//申请成功的话，得到相应的任务类型和文件名
		if reply.MapOrReduce==0{
			
			filename := reply.FileName
			// fmt.Printf("maptask%v :filename: %v\n",index,filename)
			
			HashBucket :=make([]*os.File, R)
			for i:=0;i<R;i++{
				//分配hash桶，创建相应的中间文件
				//首先创建的文件应该是临时文件
				mapFileName :=fmt.Sprintf("mr-%v-%v", index,i)
				f, err := ioutil.TempFile("./", mapFileName)
				//创建中间文件为之后写入做准备
				HashBucket[i] = f
				if err != nil {
					log.Fatalf("cannot create tempFile %v", mapFileName)
				}
			}
			//若分配的是一个maptask，把将指定的filename执行map函数并根据key写入hash桶中
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("maptask %v cannot open %v", index,filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			//执行mapf获得所有的kv键值对
			kva := mapf(filename, string(content))
			for _, kv := range kva {
				hash := ihash(kv.Key)% R
				enc := json.NewEncoder(HashBucket[hash])
				//写入追加到文件中
				err := enc.Encode(&kv)
				if err != nil{
					log.Fatalf("kv cannot encode %v:%v", kv.Key,kv.Value)
				}
			}
			//最后需要关闭所有的临时文件流,重命名
			for i:=0;i<R;i++{
				oldPath := HashBucket[i].Name()
				newPath := fmt.Sprintf("mr-%v-%v", index,i)
				HashBucket[i].Close()
				os.Rename(oldPath,newPath)
				
			}
			//结束前需要告知master，该mapTask已经完成
			CallMapComplete: mArgs := MapCompleteArgs{}
			mArgs.TaskNumber = index
			mReply := MapCompleteReply{}
			ok0 :=call("Coordinator.MapComplete",&mArgs,&mReply)
			
			if !ok0{
				log.Fatalf("%v call MapComplete fail\n",index)
				fmt.Printf("call MapComplete fail\n")
				time.Sleep(time.Second)
				goto CallMapComplete
			}
			// fmt.Printf("maptask %v complete\n",index)
		}else if reply.MapOrReduce == 1{
			//若申请到一个reduce任务,获取reduce任务编号
			//判断map任务是否全部完成了
			LOOP:rArgs:=ReadyForReduceArgs{}
			rArgs.TaskNumber=index
			rReply:=ReadyForReduceReply{}
			 ok1 := call("Coordinator.ReadyForReduce", &rArgs, &rReply)
			if ok1 {
				if !rReply.Ready {
					// fmt.Printf("not ready for reduce\n")
					if rReply.MapRet{
						// fmt.Printf("map task reset, so apply for new task\n")
						goto ApplyForTask
					}
					time.Sleep(time.Second)
					goto LOOP
				}
				//全部的maptask已经完成，首先取出临时文件中的所有kv
				intermediate := []KeyValue{}
				for i:=0;i<M;i++{
					name := fmt.Sprintf("mr-%v-%v", i, index)
					file,err:=os.Open(name)
					if err != nil {
						log.Fatalf("cannot read %v", name)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							// log.Fatalf("cannot read kv %v", name)
							break
						}
						intermediate = append(intermediate, kv)
					}
						
				}
				sort.Sort(ByKey(intermediate))
				//先创建临时文件
				finalFileName := fmt.Sprintf("mr-out-%v",index)
				final, err := ioutil.TempFile("./", finalFileName)
				if err!=nil{
					log.Fatalf("cannot creat temp  %v", finalFileName)
				}
				//执行reduce函数
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(final, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				//最后重命名
				oldPath := final.Name()
				// fmt.Printf("final:%v",oldPath)
				// newPath := fmt.Sprintf("mr-%v-%v", index,i)
				final.Close()
				os.Rename(oldPath,finalFileName)
				
				//reduce任务结束，向master报备
				CallReduceComplete: rArgs:=ReduceCompleteArgs{}
				rArgs.TaskNumber=index
				rReply:=ReduceCompleteReply{}
				complete :=call("Coordinator.ReduceComplete", &rArgs, &rReply)
				if !complete{
					log.Fatalf("call ReduceComplete fail\n")
					fmt.Printf("call ReduceComplete fail\n")
					goto CallReduceComplete
				}
				

			}

		}else if reply.MapOrReduce == -1{
			//这表示已无task可以分配,等待后再次发起申请
			// fmt.Printf("no task\n")
			time.Sleep(time.Second)
			goto ApplyForTask
	
		}
	} else {
		// log.Fatalf("call ApplyForTask failed!\n")
		//coordinator进程已退出,worker节点也退出即可
		fmt.Printf("与coordinator失去连接!\n")
		os.Exit(0)

	}
	//worker处理完一个任务以后应该可以重复利用
	// fmt.Printf("重复利用\n")
	// time.Sleep(time.Second)
	goto ApplyForTask

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
