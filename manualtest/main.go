package main

import (
	"fmt"
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

func main() {
	fmt.Println()
	//fmt.Println(productExceptSelf([]int{1, 2, 3, 4}))
	//fmt.Println(productExceptSelf([]int{-1, 1, 0, -3, 3}))
	//fmt.Println(maxSlidingWindow([]int{1, 3, 1, 2, 0, 5}, 3))
	nums := []int{0, 0, 1, 0, 0, 1}
	moveZeroes(nums)
	fmt.Println(nums)
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

func moveZeroes(nums []int) {
	var (
		i    int
		zero int
	)
	for i = 0; i < len(nums); i++ {
		if nums[i] == 0 {
			zero++
			continue
		}
		nums[i-zero] = nums[i]
	}
	for j := len(nums) - 1; j >= len(nums)-zero; j-- {
		nums[j] = 0
	}
}
