package main

import (
	"fmt"
)

type Node struct {
	Val    int
	Next   *Node
	Random *Node
}

func main() {
	fmt.Println(buildTree([]int{1, 2}, []int{2, 1}))
}

func copyRandomList(head *Node) *Node {
	var arr []*Node
	mapping := make(map[int64]*Node)
	for n := head; n != nil; n = n.Next {
		arr = append(arr, n)
	}
	arr2 := make([]*Node, len(arr))
	var ret *Node
	var n *Node
	for i := 0; i < len(arr); i++ {
		var newNode *Node
		node := arr[i]
		if arr2[node.Random] != nil {

		}
		if i == 0 {
			ret = &Node{
				Val:  arr[i].Val,
				Next: nil,
			}
		}
	}
}
