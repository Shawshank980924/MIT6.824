package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapCompleteArgs struct{
	TaskNumber int
}

type MapCompleteReply struct{
	Ok bool
}

type ReduceCompleteArgs struct{
	TaskNumber int
}

type ReduceCompleteReply struct{
	Ok bool
}

type ReadyForReduceArgs struct{
	TaskNumber int
}

type ReadyForReduceReply struct{
	//ready表明已经准备好reduce了
	Ready bool
	//表明map中有task超时已经重置idle，此时reduce task应该重新applyForTask
	MapRet bool
}

// Add your RPC definitions here.
type ApplyForTaskArgs struct {
	ReadyForApply bool
}

type ApplyForReply struct {
	//0表示mapf 1表示reducef
	MapOrReduce int
	//被分配到的tasknumer
	TaskNumber int
	//总的maptasks和总的reducetasks
	//对于mapf写中间文件以及hash分桶需要这两个参数
	//对于reducef可以不需要
	TotMapTasks int
	TotReduceTasks int
	//对于map来说是指输入的文件名
	//对于reduce来说是指
	FileName string
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
