/*
 * @Author: Shawshank980924 Akatsuki980924@163.com
 * @Date: 2022-07-30 03:04:24
 * @LastEditors: Shawshank980924 Akatsuki980924@163.com
 * @LastEditTime: 2022-08-03 20:33:54
 * @FilePath: /MIT6.824/src/6.824/src/mr/coordinator.go
 * @Description:
 *
 * Copyright (c) 2022 by Shawshank980924 Akatsuki980924@163.com, All Rights Reserved.
 */
package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	//mapTask和reduceTask的总数
	MapTaskNum int
	ReduceTaskNum int 
	//当前完成
	//维护每个map和reduce task的状态,0表示idle，1表示in-progess，2表示complete
	MapTaskState []int
	ReduceTaskState []int
	//每个task开始的时间戳
	MapTaskStart []int64
	ReduceTaskStart []int64
	//已经完成的task
	CompleteMapNum int
	CompleteReduceNum int
	//mapTask对应的fileName
	MapFile []string
	//共享资源需要维护一把读写锁
	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) MapComplete(args *MapCompleteArgs, reply *MapCompleteReply) error {
	c.Lock()
	c.CompleteMapNum ++
	c.MapTaskState[args.TaskNumber] =2
	reply.Ok =true
	defer c.Unlock()
	return nil
}

func (c *Coordinator) ReduceComplete(args *ReduceCompleteArgs, reply *ReduceCompleteReply) error {
	c.Lock()
	c.CompleteReduceNum ++
	c.ReduceTaskState[args.TaskNumber] =2
	reply.Ok = true
	defer c.Unlock()
	return nil
}

func (c *Coordinator) ReadyForReduce(args *ReadyForReduceArgs, reply *ReadyForReduceReply) error {
	
	reply.Ready =false
	reply.MapRet=false
	index := args.TaskNumber
	c.Lock()
	c.ReduceTaskStart[index] = time.Now().Unix()
	if c.CompleteMapNum == c.MapTaskNum{
		reply.Ready =true
	}else{
		//需要检查map task是否有超时，若超时则重置
		for i,mapState := range(c.MapTaskState){
			if mapState == 1 {
				//对于in-progress的task，查看运行时间若超过10s直接重置任务
				now := time.Now().Unix()
				if now > c.MapTaskStart[i]+10{
					c.MapTaskState[i]=0
					c.ReduceTaskState[index]=0
					fmt.Printf("map task%v reduce task%v reset\n",i,index)
					defer c.Unlock()
					reply.MapRet =true
					return nil
				}
	
			}
		}
	}
	
	defer c.Unlock()
	return nil
}

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForReply) error {
	//对共享资源采用互斥锁
	c.Lock()
	reply.TotMapTasks = c.MapTaskNum
	reply.TotReduceTasks = c.ReduceTaskNum
	//先看是否可以分配mapTask
	for i,mapState := range(c.MapTaskState){
		if mapState == 0{
			//若该task处于idle状态
			reply.FileName = c.MapFile[i]
			// fmt.Printf("map %v: %v\n",i,reply.FileName)
			reply.MapOrReduce = 0
			reply.TaskNumber = i
			
			//将该状态置为in-progress
			c.MapTaskState[i] = 1
			//设置任务开始时间
			c.MapTaskStart[i] =time.Now().Unix()
			defer c.Unlock()
			return nil
		}else if mapState == 1 {
			//对于in-progress的task，查看运行时间若超过10s直接重置任务
			now := time.Now().Unix()
			if now > c.MapTaskStart[i]+10{
				reply.FileName = c.MapFile[i]
				reply.MapOrReduce = 0
				reply.TaskNumber = i
				c.MapTaskStart[i] = now
				defer c.Unlock()
				return nil
			}

		}
	}

	//map任务已全部分配结束，则分配reduce任务
	for i,reduceState := range(c.ReduceTaskState){
		if reduceState==0 {
			//该reduce task属于idle
			reply.MapOrReduce=1
			reply.TaskNumber=i
			c.ReduceTaskState[i] = 1
			c.ReduceTaskStart[i] = time.Now().Unix()
			defer c.Unlock()
			return nil
		}else if reduceState==1{
			//对于in-progress的task，查看运行时间若超过10s直接重置任务
			now := time.Now().Unix()
			if now > c.ReduceTaskStart[i]+10{
				reply.MapOrReduce = 1
				reply.TaskNumber = i
				c.ReduceTaskStart[i] = now
				defer c.Unlock()
				return nil
			}
		}
	}
	//当前没有任务可以分配
	reply.MapOrReduce =-1
	defer c.Unlock()
	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	
	// Your code here.
	//若map和reduce task全部完成了则ret置为true
	c.Lock()
	// fmt.Printf("CompleteMapNum: %v, CompleteReduceNum: %v\n",c.CompleteMapNum,c.CompleteReduceNum)
	if c.CompleteMapNum==c.MapTaskNum && c.CompleteReduceNum==c.ReduceTaskNum{
		fmt.Printf("all done\n")
		ret = true
	}
	defer c.Unlock()


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//nReduce就相当于论文中的R，表示分区数量，必然需要记录在coordinator的结构体中
	//协调器负责
	nMap:=len(files)
	c.MapTaskNum = nMap
	c.ReduceTaskNum = nReduce
	c.MapTaskState = make([]int,nMap)
	c.ReduceTaskState = make([]int,nReduce)
	c.MapTaskStart = make([]int64,nMap)
	c.ReduceTaskStart= make([]int64,nReduce)
	c.CompleteMapNum = 0
	c.CompleteReduceNum = 0
	c.MapFile = make([]string, nMap)
	for i,fileName := range(files){
		c.MapFile[i] = fileName
		// fmt.Printf("mapfile %v : %v\n",i,fileName)
	}
	c.server()
	return &c
}
