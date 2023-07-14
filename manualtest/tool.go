package main

import (
	"fmt"
	"math"
)

type TreeNode struct {
	Val   int
	Left  *TreeNode
	Right *TreeNode
}

type ListNode struct {
	Val  int
	Next *ListNode
}

func pre(root *TreeNode) {
	if root == nil || root.Val == wild {
		return
	}
	fmt.Println(root.Val)
	pre(root.Left)
	pre(root.Right)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

const wild = math.MaxInt64

//func buildTree(nums []int, i int) *TreeNode {
//	if i >= len(nums) || nums[i] == wild {
//		return nil
//	}
//	t := &TreeNode{Val: nums[i]}
//	t.Left = buildTree(nums, 2*i+1)
//	t.Right = buildTree(nums, 2*i+2)
//	return t
//}

func buildList(nums []int) *ListNode {
	head := &ListNode{Val: nums[0]}
	cur := head
	for i := 1; i < len(nums); i++ {
		cur.Next = &ListNode{Val: nums[i]}
		cur = cur.Next
	}
	return head
}

func printList(head *ListNode) (res []int) {
	for head != nil {
		res = append(res, head.Val)
		head = head.Next
	}
	return res
}
